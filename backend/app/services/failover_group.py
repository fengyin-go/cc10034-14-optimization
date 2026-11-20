"""故障切换领域服务：管理CDC通道的主备切换。

上游：cdc_channel + checkpoint_manager 产出通道状态和检查点 → failover_group 消费
消费：position_store 根据切换结果更新当前位点；恢复回执同步回 position_store 供重试核验

切换校验优化：对主备 GTID 区间按来源归并（消除重复计数），产生可作为恢复依据的
切换覆盖数据集；只把数据集中已获确认且依赖齐备的范围作为恢复输入。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.gtid import (
    GtidInterval,
    covers,
    expand,
    merge_intervals,
    naive_count,
    parse_gtid_intervals,
    subtract,
    total_count,
)
from app.services.base import BaseModuleService
from app.services.position_store import PositionStoreService
from app.pipeline import pipeline_bus

# 依赖键/值都用 (来源, 事务编号) 二元组表示，例如 ("orders", 4) 依赖 [("inventory", 4)]
TxKey = tuple[str, int]
DependencyMap = dict[TxKey, list[TxKey]]


def _tx_label(tx: TxKey) -> str:
    return f"{tx[0]}:{tx[1]}"


def _int_or(value: Any, default: int) -> int:
    """容错解析整数配置：样例数据里可能是非数字文本，回退到默认值。"""
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


def _intervals_from_dicts(items: list[dict[str, Any]]) -> list[GtidInterval]:
    return [GtidInterval(d["source"], d["start"], d["end"]) for d in items]


class FailoverGroupService(BaseModuleService):
    MODULE = "failover_group"
    ENTITY = "切换组"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["组名", "主通道", "备用通道", "切换条件", "自动切换", "心跳超时", "上次切换时间"]
    REQUIRED_FIELDS = ["组名", "主通道", "备用通道"]
    STATUS_ORDER = ["主运行", "备运行", "切换中", "已失效"]
    ACTION_RULES = {"执行切换": "主运行", "标记失效": "主运行"}
    NEGATIVE_ACTIONS = []

    # ------------------------------------------------------------------
    # 切换覆盖数据集
    # ------------------------------------------------------------------
    def build_switchover_coverage(self, primary_instance: str,
                                  standby_instance: str) -> dict[str, Any]:
        """归并主备 GTID 区间，产生可作为恢复依据的切换覆盖数据集。

        - 期望区间：主备区间归并后的并集（同一来源重叠只计一次，异源同编号分别计算）；
        - 已确认区间：备用端实际持有的归并区间，是恢复输入的唯一来源；
        - 缺口区间：期望 − 已确认，用于告警阈值判定。
        """
        primary_pos = pipeline_bus.get_position(primary_instance)
        standby_pos = pipeline_bus.get_position(standby_instance)
        primary_intervals = parse_gtid_intervals(primary_pos.gtid_set) if primary_pos else []
        standby_intervals = parse_gtid_intervals(standby_pos.gtid_set) if standby_pos else []

        expected = merge_intervals([*primary_intervals, *standby_intervals])
        confirmed = merge_intervals(standby_intervals)
        gaps = subtract(expected, confirmed)

        sources: dict[str, Any] = {}
        for src in sorted(expected):
            raw = [iv for iv in [*primary_intervals, *standby_intervals] if iv.source == src]
            exp_count = sum(iv.count for iv in expected.get(src, []))
            sources[src] = {
                "expected": [iv.to_dict() for iv in expected.get(src, [])],
                "confirmed": [iv.to_dict() for iv in confirmed.get(src, [])],
                "gap": [iv.to_dict() for iv in gaps.get(src, [])],
                "expected_count": exp_count,
                "confirmed_count": sum(iv.count for iv in confirmed.get(src, [])),
                "gap_count": sum(iv.count for iv in gaps.get(src, [])),
                # 旧逻辑直接累加原始区间计数，这里给出被消除的重复计数
                "duplicates_eliminated": naive_count(raw) - exp_count,
            }
        return {
            "primary": primary_instance,
            "standby": standby_instance,
            "sources": sources,
            "total_expected": total_count(expected),
            "total_confirmed": total_count(confirmed),
            "total_gap": total_count(gaps),
            "generated_at": datetime.now().isoformat(),
        }

    # ------------------------------------------------------------------
    # 恢复计划
    # ------------------------------------------------------------------
    def plan_recovery(self, coverage: dict[str, Any],
                      dependencies: DependencyMap | None = None,
                      alert_threshold: int = 1) -> dict[str, Any]:
        """基于切换覆盖数据集规划恢复。

        只把已获确认且依赖齐备的事务纳入恢复输入：
        - recoverable：备用端已确认覆盖、且依赖的所有来源事务都已覆盖；
        - blocked：本身已覆盖，但依赖的事务落在缺口里，必须等所需来源都覆盖后再恢复；
        - missing：本身落在缺口里，备用端未接管。
        缺口事务数达到告警阈值时触发告警，判定从「完整移交」降级为「部分接管」。
        """
        deps = dependencies or {}
        expected = merge_intervals(
            iv for src in coverage["sources"].values()
            for iv in _intervals_from_dicts(src["expected"])
        )
        confirmed = merge_intervals(
            iv for src in coverage["sources"].values()
            for iv in _intervals_from_dicts(src["confirmed"])
        )

        recoverable: list[TxKey] = []
        blocked: list[dict[str, Any]] = []
        missing: list[TxKey] = []
        for src in sorted(expected):
            for n in expand(expected, src):
                tx = (src, n)
                if not covers(confirmed, src, n):
                    missing.append(tx)
                    continue
                waiting_on = [d for d in deps.get(tx, []) if not covers(confirmed, d[0], d[1])]
                if waiting_on:
                    blocked.append({"tx": tx, "waiting_on": waiting_on})
                else:
                    recoverable.append(tx)

        gap_count = coverage["total_gap"]
        alert_triggered = gap_count >= max(alert_threshold, 1)
        decision = "完整移交" if not missing and not blocked else "部分接管"
        return {
            "recoverable": [list(tx) for tx in recoverable],
            "recoverable_count": len(recoverable),
            "blocked": [
                {"tx": list(b["tx"]), "waiting_on": [list(d) for d in b["waiting_on"]]}
                for b in blocked
            ],
            "blocked_count": len(blocked),
            "missing": [list(tx) for tx in missing],
            "missing_count": len(missing),
            "alert": {
                "threshold": alert_threshold,
                "gap_count": gap_count,
                "triggered": alert_triggered,
                "gap_transactions": [_tx_label(tx) for tx in missing],
            },
            "decision": decision,
        }

    # ------------------------------------------------------------------
    # 恢复执行（回执同步位点存储，重试不重复应用）
    # ------------------------------------------------------------------
    def execute_recovery(self, plan: dict[str, Any], standby_instance: str,
                         position_store: PositionStoreService | None = None,
                         fail_after: int | None = None) -> dict[str, Any]:
        """执行恢复：仅应用计划内事务，回执逐笔同步位点存储。

        重试时先读取位点存储中已确认的回执，已确认的事务直接跳过，
        保证失败重试不会重复应用。fail_after 用于模拟恢复到一半崩溃。
        """
        ps_svc = position_store or PositionStoreService()
        already = ps_svc.recovered_coverage(standby_instance)
        applied: list[TxKey] = []
        skipped: list[TxKey] = []
        completed = True
        for src, n in (tuple(tx) for tx in plan["recoverable"]):
            tx = (src, n)
            if covers(already, src, n):
                skipped.append(tx)
                continue
            if fail_after is not None and len(applied) >= fail_after:
                completed = False
                break
            ps_svc.sync_recovery_receipt(standby_instance, [GtidInterval(src, n, n)])
            already = ps_svc.recovered_coverage(standby_instance)
            applied.append(tx)

        result = {
            "standby": standby_instance,
            "applied": [_tx_label(tx) for tx in applied],
            "applied_count": len(applied),
            "skipped_confirmed": [_tx_label(tx) for tx in skipped],
            "skipped_count": len(skipped),
            "blocked": [_tx_label(tuple(b["tx"])) for b in plan["blocked"]],
            "completed": completed,
        }
        pipeline_bus.write_audit(
            "故障切换", standby_instance,
            f"恢复应用 {len(applied)} 笔，跳过已确认 {len(skipped)} 笔"
            + ("" if completed else "，中断待重试"),
            "failover_group", "已恢复" if completed else "恢复中断"
        )
        return result

    # ------------------------------------------------------------------
    # 切换核验：对照前后位点，验证恢复集合与缺口判定
    # ------------------------------------------------------------------
    def verify_switchover(self, config: dict[str, Any],
                          dependencies: DependencyMap | None = None,
                          alert_threshold: int = 1,
                          fail_after: int | None = None) -> dict[str, Any]:
        """核验一次切换：构建覆盖数据集 → 规划 → 恢复（可模拟中断重试）→ 前后位点对照。"""
        primary = config["主通道"]
        standby = config["备用通道"]
        ps_svc = PositionStoreService()

        before = {
            "primary": ps_svc.source_coverage(primary),
            "standby": ps_svc.source_coverage(standby),
            "standby_recovered": ps_svc.sync_recovery_receipt(standby, []),
        }

        coverage = self.build_switchover_coverage(primary, standby)
        plan = self.plan_recovery(coverage, dependencies, alert_threshold)

        runs = [self.execute_recovery(plan, standby, ps_svc, fail_after=fail_after)]
        if not runs[-1]["completed"]:
            # 失败重试：已确认事务由位点存储回执核验后跳过
            runs.append(self.execute_recovery(plan, standby, ps_svc))

        after = {
            "primary": ps_svc.source_coverage(primary),
            "standby": ps_svc.source_coverage(standby),
            "standby_recovered": ps_svc.sync_recovery_receipt(standby, []),
        }

        applied_all = [tx for run in runs for tx in run["applied"]]
        planned = {_tx_label(tuple(tx)) for tx in plan["recoverable"]}
        blocked_labels = {_tx_label(tuple(b["tx"])) for b in plan["blocked"]}
        missing_labels = set(plan["alert"]["gap_transactions"])
        recovered_total = after["standby_recovered"]["recovered_total"]

        checks = {
            # 实际恢复集合 == 计划恢复集合
            "recovered_matches_plan": set(applied_all) == planned,
            # 失败重试没有重复应用已确认事务
            "no_duplicate_apply": len(applied_all) == len(set(applied_all)),
            # 被依赖阻塞的事务未进入恢复
            "blocked_not_recovered": not (set(applied_all) & blocked_labels),
            # 缺口事务未进入恢复
            "gap_not_recovered": not (set(applied_all) & missing_labels),
            # 回执总数与恢复集合一致（位点存储已同步）
            "receipts_match_recovery": recovered_total == len(planned),
        }
        return {
            "group": config.get("组名", ""),
            "primary": primary,
            "standby": standby,
            "coverage": coverage,
            "plan": plan,
            "runs": runs,
            "before": before,
            "after": after,
            "checks": checks,
            "all_passed": all(checks.values()),
        }

    # ------------------------------------------------------------------
    # 既有切换流程：接入覆盖校验
    # ------------------------------------------------------------------
    def execute_failover(self, config: dict[str, Any]) -> dict[str, Any]:
        """执行主备切换。

        根据切换条件评估是否需要切换：
        - 主通道心跳超时
        - 主通道检查点停滞
        - 手动触发
        切换触发后，若位点存储已有主备位点，则附带切换覆盖校验结论。
        """
        group_name = config["组名"]
        primary_channel = config["主通道"]
        standby_channel = config["备用通道"]
        switch_condition = config.get("切换条件", "heartbeat_timeout")
        auto_switch = config.get("自动切换", "否") == "是"
        heartbeat_timeout = _int_or(config.get("心跳超时"), 30)
        last_switch = config.get("上次切换时间", "")

        should_switch = self._evaluate_condition(
            switch_condition, primary_channel, heartbeat_timeout
        )

        result: dict[str, Any] = {
            "group": group_name,
            "primary": primary_channel,
            "standby": standby_channel,
            "switch_triggered": False,
            "switch_reason": "",
            "new_primary": primary_channel,
        }

        if should_switch or not auto_switch:
            result["switch_triggered"] = True
            result["switch_reason"] = "heartbeat_timeout" if should_switch else "manual"
            result["new_primary"] = standby_channel
            result["new_standby"] = primary_channel
            result["switched_at"] = datetime.now().isoformat()
            result["last_switch"] = result["switched_at"]

            if pipeline_bus.get_position(primary_channel) and pipeline_bus.get_position(standby_channel):
                alert_threshold = _int_or(config.get("告警阈值"), 1)
                coverage = self.build_switchover_coverage(primary_channel, standby_channel)
                plan = self.plan_recovery(coverage, alert_threshold=alert_threshold)
                result["coverage_summary"] = {
                    src: {
                        "confirmed_count": info["confirmed_count"],
                        "gap_count": info["gap_count"],
                        "duplicates_eliminated": info["duplicates_eliminated"],
                    }
                    for src, info in coverage["sources"].items()
                }
                result["recovery_decision"] = plan["decision"]
                result["gap_alert"] = plan["alert"]["triggered"]
                result["recoverable_count"] = plan["recoverable_count"]

            pipeline_bus.write_audit(
                "故障切换", group_name,
                f"切换组 {group_name} 主通道从 {primary_channel} 切换至 {standby_channel}",
                "failover_group", "已切换"
            )
        else:
            result["switch_reason"] = "healthy"
            result["switch_triggered"] = False

        return result

    def _evaluate_condition(self, condition: str, channel: str, timeout: int) -> bool:
        """评估切换条件。"""
        if condition == "heartbeat_timeout":
            # 检查通道位点的最后更新时间
            pos = pipeline_bus.get_position(channel)
            if pos is None:
                return True
            elapsed = (datetime.now() - pos.updated_at).total_seconds()
            return elapsed > timeout
        if condition == "checkpoint_stale":
            commits = [cp for cp in pipeline_bus.checkpoints
                       if cp.channel_name == channel and cp.committed]
            if not commits:
                return True
            latest = commits[-1]
            elapsed = (datetime.now() - latest.confirmed_at).total_seconds()
            return elapsed > timeout * 3
        if condition == "always":
            return True
        return False

    def heartbeat_check(self, channel_name: str, timeout_sec: float = 30.0) -> dict[str, Any]:
        """心跳检查。"""
        pos = pipeline_bus.get_position(channel_name)
        healthy = True
        elapsed = 0.0
        if pos:
            elapsed = (datetime.now() - pos.updated_at).total_seconds()
            healthy = elapsed <= timeout_sec
        else:
            elapsed = -1

        return {
            "channel": channel_name,
            "healthy": healthy,
            "last_seen_sec_ago": round(elapsed, 1),
            "timeout_sec": timeout_sec,
            "needs_failover": not healthy and channel_name in {
                cp.channel_name for cp in pipeline_bus.checkpoints
            },
        }

    def compute_failover_risk(self, config: dict[str, Any]) -> dict[str, Any]:
        """计算故障风险评分。"""
        primary = config["主通道"]
        heartbeat = self.heartbeat_check(primary)
        checkpoint_commits = sum(1 for cp in pipeline_bus.checkpoints
                                 if cp.channel_name == primary and cp.committed)
        risk_score = 0
        if not heartbeat["healthy"]:
            risk_score += 50
        if checkpoint_commits == 0:
            risk_score += 30
        elapsed = heartbeat.get("last_seen_sec_ago", -1)
        if elapsed > 120:
            risk_score += 20
        risk_level = "低" if risk_score < 30 else ("中" if risk_score < 60 else "高")
        return {
            "group": config["组名"],
            "primary_channel": primary,
            "risk_score": min(risk_score, 100),
            "risk_level": risk_level,
            "healthy": heartbeat["healthy"],
            "checkpoint_commits": checkpoint_commits,
            "recommends_manual_check": risk_score >= 50,
        }

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry and action == "执行切换":
            result = self.execute_failover(entry)
            if result["switch_triggered"]:
                entry["status"] = "切换中"
        return entry, msg
