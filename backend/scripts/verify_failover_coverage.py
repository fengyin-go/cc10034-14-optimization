"""故障切换覆盖校验的场景核验脚本。

场景：一次切换同时涉及订单(orders)和库存(inventory)两个事务来源。
- 主库位点：orders:1-8，inventory:1-8（切换前的完整事务区间）
- 备库位点：orders 由相交区间 1-5 与 4-8 覆盖（归并后 1-8 已齐）；
  inventory 只有 1-3 与 6-8（缺 4-5 一段）
- 依赖：orders:4 引用 inventory:4（库存记录未到齐，必须等待）；
  orders:8 不引用任何库存记录，可独立复原
- 告警阈值：缺口事务数 ≥ 2 触发告警，判定降级为「部分接管」
- 恢复模拟：第一轮应用到第 3 笔后崩溃，重试时由位点存储回执核验，
  已确认事务不得重复应用

运行：cd backend && .venv/bin/python scripts/verify_failover_coverage.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.gtid import merge_intervals, naive_count, parse_gtid_intervals, total_count
from app.pipeline import pipeline_bus
from app.services.failover_group import FailoverGroupService
from app.services.position_store import PositionStoreService

PRIMARY = "mysql-primary"
STANDBY = "mysql-standby"

pos_svc = PositionStoreService()
fo_svc = FailoverGroupService()

failures: list[str] = []


def check(name: str, actual, expected) -> None:
    ok = actual == expected
    print(f"  [{'✓' if ok else '✗'}] {name}: 实际={actual} 期望={expected}")
    if not ok:
        failures.append(name)


def show(title: str) -> None:
    print(f"\n【{title}】")


# ---------------------------------------------------------------------
# 1. 位点存储按来源登记主备 GTID 覆盖
# ---------------------------------------------------------------------
show("1. 位点存储：按来源登记主备 GTID 覆盖")
pos_svc.save_source_coverage(
    PRIMARY,
    {"orders": [(1, 8)], "inventory": [(1, 8)]},
    binlog_name="mysql-bin.000420", position=98112, source="primary",
)
pos_svc.save_source_coverage(
    STANDBY,
    {"orders": [(1, 5), (4, 8)], "inventory": [(1, 3), (6, 8)]},
    binlog_name="mysql-bin.000420", position=87455, source="standby",
)
print(f"  主库 Gtid集合: {pipeline_bus.get_position(PRIMARY).gtid_set}")
print(f"  备库 Gtid集合: {pipeline_bus.get_position(STANDBY).gtid_set}")

# 旧逻辑（未归并）会重复计数：orders 1-5 与 4-8 被算成 10 笔
raw_standby = parse_gtid_intervals(pipeline_bus.get_position(STANDBY).gtid_set)
merged_standby = merge_intervals(raw_standby)
check("旧逻辑备库订单原始计数（含重复）", naive_count(
    [iv for iv in raw_standby if iv.source == "orders"]), 10)
check("归并后备库订单去重计数", sum(iv.count for iv in merged_standby["orders"]), 8)
check("异源同编号分别计算（订单8 + 库存6）", total_count(merged_standby), 14)

# ---------------------------------------------------------------------
# 2. 切换覆盖数据集：归并主备区间，校验事务区间
# ---------------------------------------------------------------------
show("2. 切换覆盖数据集：主备归并 + 缺口校验")
coverage = fo_svc.build_switchover_coverage(PRIMARY, STANDBY)
for src, info in coverage["sources"].items():
    print(f"  {src}: 期望={info['expected']} 已确认={info['confirmed']} "
          f"缺口={info['gap']} 消除重复={info['duplicates_eliminated']}")
check("订单已确认区间", [(d["start"], d["end"]) for d in coverage["sources"]["orders"]["confirmed"]],
      [(1, 8)])
check("库存已确认区间", [(d["start"], d["end"]) for d in coverage["sources"]["inventory"]["confirmed"]],
      [(1, 3), (6, 8)])
check("库存缺口区间", [(d["start"], d["end"]) for d in coverage["sources"]["inventory"]["gap"]],
      [(4, 5)])
check("订单缺口数", coverage["sources"]["orders"]["gap_count"], 0)
check("订单消除的重复计数（主8+备10→8）", coverage["sources"]["orders"]["duplicates_eliminated"], 10)
check("库存缺口事务数", coverage["sources"]["inventory"]["gap_count"], 2)
check("数据集已确认总数", coverage["total_confirmed"], 14)

# ---------------------------------------------------------------------
# 3. 恢复计划：依赖范围对应备用端实际接管的事务
# ---------------------------------------------------------------------
show("3. 恢复计划：依赖等待 + 告警阈值判定")
dependencies = {("orders", 4): [("inventory", 4)]}  # 订单第4笔引用库存第4笔；订单第8笔独立
plan = fo_svc.plan_recovery(coverage, dependencies=dependencies, alert_threshold=2)
print(f"  可恢复({plan['recoverable_count']}): "
      f"{[f'{s}:{n}' for s, n in plan['recoverable']]}")
print(f"  阻塞等待({plan['blocked_count']}): {plan['blocked']}")
print(f"  缺口事务({plan['missing_count']}): {plan['alert']['gap_transactions']}")
print(f"  告警: 阈值={plan['alert']['threshold']} 缺口={plan['alert']['gap_count']} "
      f"触发={plan['alert']['triggered']}  判定={plan['decision']}")
check("可恢复事务数（订单7 + 库存6）", plan["recoverable_count"], 13)
check("订单第4笔被依赖阻塞", plan["blocked"],
      [{"tx": ["orders", 4], "waiting_on": [["inventory", 4]]}])
check("订单第8笔独立可恢复", ["orders", 8] in plan["recoverable"], True)
check("缺口达到阈值触发告警", plan["alert"]["triggered"], True)
check("缺口与完整移交冲突时判定部分接管", plan["decision"], "部分接管")

# ---------------------------------------------------------------------
# 4. 恢复执行：模拟中断重试，回执同步位点存储
# ---------------------------------------------------------------------
show("4. 恢复执行：第3笔后崩溃 → 重试不重复应用")
run1 = fo_svc.execute_recovery(plan, STANDBY, fail_after=3)
print(f"  第一轮: 应用={run1['applied']} 完成={run1['completed']}")
check("第一轮应用3笔后中断", (run1["applied_count"], run1["completed"]), (3, False))

run2 = fo_svc.execute_recovery(plan, STANDBY)
print(f"  第二轮: 跳过已确认={run2['skipped_confirmed']}")
print(f"  第二轮: 新应用={run2['applied']}")
check("重试跳过第一轮已确认事务", run2["skipped_confirmed"], run1["applied"])
check("重试补应用剩余10笔", run2["applied_count"], 10)
check("两轮无重复应用", len(set(run1["applied"]) & set(run2["applied"])), 0)
check("回执已同步位点存储", pos_svc.sync_recovery_receipt(STANDBY, [])["recovered_total"], 13)

# ---------------------------------------------------------------------
# 5. 切换前后位点对照（完整核验流程）
# ---------------------------------------------------------------------
show("5. 对照切换前后位点，核验恢复集合与缺口判定")
# 换一组干净实例跑完整 verify_switchover（含中断重试与前后对照）
pos_svc.save_source_coverage(
    "mysql-primary-2", {"orders": [(1, 8)], "inventory": [(1, 8)]},
    binlog_name="mysql-bin.000420", position=98112, source="primary",
)
pos_svc.save_source_coverage(
    "mysql-standby-2", {"orders": [(1, 5), (4, 8)], "inventory": [(1, 3), (6, 8)]},
    binlog_name="mysql-bin.000420", position=87455, source="standby",
)
report = fo_svc.verify_switchover(
    {"组名": "订单库存切换组", "主通道": "mysql-primary-2", "备用通道": "mysql-standby-2"},
    dependencies=dependencies, alert_threshold=2, fail_after=3,
)
before_rec = report["before"]["standby_recovered"]["recovered_total"]
after_rec = report["after"]["standby_recovered"]["recovered"]
after_summary = {src: [(iv["start"], iv["end"]) for iv in ivs] for src, ivs in after_rec.items()}
print(f"  切换前已恢复: {before_rec} 笔")
print(f"  切换后已恢复: {after_summary}")
for name, ok in report["checks"].items():
    print(f"  [{'✓' if ok else '✗'}] {name}")
check("切换前无已恢复事务", before_rec, 0)
check("切换后订单恢复区间（第4笔仍阻塞）",
      [(iv["start"], iv["end"]) for iv in after_rec["orders"]], [(1, 3), (5, 8)])
check("切换后库存恢复区间（缺口4-5未恢复）",
      [(iv["start"], iv["end"]) for iv in after_rec["inventory"]], [(1, 3), (6, 8)])
check("核验全部通过", report["all_passed"], True)

# ---------------------------------------------------------------------
# 6. 对照组：备用端区间完整时判定完整移交
# ---------------------------------------------------------------------
show("6. 对照组：备用端无缺口时判定完整移交")
pos_svc.save_source_coverage(
    "mysql-standby-3", {"orders": [(1, 8)], "inventory": [(1, 8)]},
    binlog_name="mysql-bin.000420", position=98112, source="standby",
)
full_coverage = fo_svc.build_switchover_coverage(PRIMARY, "mysql-standby-3")
full_plan = fo_svc.plan_recovery(full_coverage, dependencies=dependencies, alert_threshold=2)
check("无缺口不触发告警", full_plan["alert"]["triggered"], False)
check("无缺口判定完整移交", full_plan["decision"], "完整移交")
check("完整移交时订单第4笔可恢复", ["orders", 4] in full_plan["recoverable"], True)

show("核验结果")
if failures:
    print(f"  未通过 {len(failures)} 项: {failures}")
    sys.exit(1)
print("  全部核验通过 ✓")
