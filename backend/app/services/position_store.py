"""位点存储领域服务：持久化和管理Binlog位点信息。

上游：checkpoint_manager 创建检查点时联动更新位点
消费：cdc_channel 启动时查询位点；failover_group 切换时读取位点
下游：backfill_position 基于位点范围执行历史回填
"""
from __future__ import annotations

import re
from datetime import datetime
from typing import Any

from app.gtid import (
    GtidInterval,
    count_by_source,
    format_gtid_intervals,
    merge_intervals,
    parse_gtid_intervals,
    total_count,
)
from app.services.base import BaseModuleService
from app.pipeline import PositionSnapshot, pipeline_bus


class PositionStoreService(BaseModuleService):
    MODULE = "position_store"
    ENTITY = "位点记录"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["记录ID", "数据库实例", "Binlog名称", "位点值", "Gtid集合", "更新时间", "位点来源"]
    REQUIRED_FIELDS = ["记录ID", "数据库实例", "Binlog名称"]
    STATUS_ORDER = ["当前位点", "历史位点", "归档位点"]
    ACTION_RULES = {"标记历史": "当前位点", "归档位点": "历史位点"}
    NEGATIVE_ACTIONS = []

    def save_position(self, instance: str, binlog_name: str, position: int,
                      gtid_set: str = "", source: str = "manual") -> PositionSnapshot:
        """保存当前位点到管道总线。"""
        ps = PositionSnapshot(
            instance=instance,
            binlog_name=binlog_name,
            position=position,
            gtid_set=gtid_set,
            updated_at=datetime.now(),
            source=source,
        )
        pipeline_bus.register_position(ps)
        pipeline_bus.write_audit(
            "位点存储", f"{instance}:{binlog_name}",
            f"位点更新至 {binlog_name}:{position}",
            "position_store", "已记录"
        )
        return ps

    def query_current_position(self, instance: str) -> dict[str, Any]:
        """查询指定实例的当前位点。"""
        ps = pipeline_bus.get_position(instance)
        if ps is None:
            return {"instance": instance, "found": False}
        return {
            "instance": instance,
            "found": True,
            "binlog_name": ps.binlog_name,
            "position": ps.position,
            "gtid_set": ps.gtid_set,
            "updated_at": ps.updated_at.isoformat(),
            "source": ps.source,
        }

    def save_source_coverage(self, instance: str,
                             coverage: dict[str, list[tuple[int, int]]],
                             binlog_name: str = "mysql-bin.000001",
                             position: int = 0,
                             source: str = "failover") -> PositionSnapshot:
        """按事务来源登记 GTID 覆盖。

        原始区间（含重叠）原样落到位点记录，归并去重在读取侧完成，
        这样主备各自上报的区间都能保留，切换校验时再统一归并。
        """
        intervals = [
            GtidInterval(src, start, end)
            for src, ranges in coverage.items()
            for (start, end) in ranges
        ]
        gtid_set = format_gtid_intervals(intervals)
        ps = PositionSnapshot(
            instance=instance,
            binlog_name=binlog_name,
            position=position,
            gtid_set=gtid_set,
            updated_at=datetime.now(),
            source=source,
        )
        pipeline_bus.register_position(ps)
        pipeline_bus.write_audit(
            "位点存储", instance,
            f"按来源登记 GTID 覆盖 {gtid_set}",
            "position_store", "已记录"
        )
        return ps

    def source_coverage(self, instance: str) -> dict[str, Any]:
        """读取实例按来源归并后的 GTID 覆盖（已消除重复计数）。"""
        ps = pipeline_bus.get_position(instance)
        if ps is None:
            return {"instance": instance, "found": False, "sources": {}, "counts": {}, "total": 0}
        merged = merge_intervals(parse_gtid_intervals(ps.gtid_set))
        return {
            "instance": instance,
            "found": True,
            "sources": {src: [iv.to_dict() for iv in ivs] for src, ivs in merged.items()},
            "counts": count_by_source(merged),
            "total": total_count(merged),
            "binlog_name": ps.binlog_name,
            "position": ps.position,
            "updated_at": ps.updated_at.isoformat(),
        }

    def sync_recovery_receipt(self, instance: str, applied: list[GtidInterval]) -> dict[str, Any]:
        """把恢复回执同步回位点存储，供失败重试时核验已确认事务。"""
        if applied:
            pipeline_bus.record_recovery_receipt(instance, applied)
            pipeline_bus.write_audit(
                "恢复回执", instance,
                f"确认恢复 {format_gtid_intervals(applied)}",
                "position_store", "已记录"
            )
        merged = pipeline_bus.confirmed_receipts(instance)
        return {
            "instance": instance,
            "recovered": {src: [iv.to_dict() for iv in ivs] for src, ivs in merged.items()},
            "recovered_total": total_count(merged),
        }

    def recovered_coverage(self, instance: str) -> dict[str, list[GtidInterval]]:
        """实例已确认恢复的归并区间（重试核验依据）。"""
        return pipeline_bus.confirmed_receipts(instance)

    def compare_positions(self, pos_a: dict[str, Any], pos_b: dict[str, Any]) -> dict[str, Any]:
        """比较两个位点的差异。"""
        same_file = pos_a.get("binlog_name") == pos_b.get("binlog_name")
        offset_a = int(pos_a.get("position", 0))
        offset_b = int(pos_b.get("position", 0))
        lag_bytes = abs(offset_a - offset_b) if same_file else None
        return {
            "same_binlog_file": same_file,
            "offset_a": offset_a,
            "offset_b": offset_b,
            "lag_bytes": lag_bytes,
            "files_differ": not same_file,
        }

    def parse_gtid(self, gtid_str: str) -> dict[str, Any]:
        """解析 Gtid 集合字符串。"""
        uuid_set: list[str] = []
        intervals: list[dict[str, Any]] = []
        for part in gtid_str.split(","):
            part = part.strip()
            if not part:
                continue
            match = re.match(r'([a-f0-9-]+):(\d+)(?:-(\d+))?', part, re.IGNORECASE)
            if match:
                uuid_val = match.group(1)
                start = int(match.group(2))
                end = int(match.group(3)) if match.group(3) else start
                uuid_set.append(uuid_val)
                intervals.append({"uuid": uuid_val, "start": start, "end": end, "count": end - start + 1})
        return {"uuid_count": len(set(uuid_set)), "total_transactions": sum(it["count"] for it in intervals), "intervals": intervals}

    def estimate_binlog_lag_events(self, source_pos: PositionSnapshot,
                                   target_pos: PositionSnapshot, avg_event_bytes: int = 256) -> int:
        """估算位点之间的积压事件数。"""
        if source_pos.binlog_name != target_pos.binlog_name:
            return -1
        diff = abs(source_pos.position - target_pos.position)
        return diff // max(avg_event_bytes, 1)

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry and action == "标记历史":
            pipeline_bus.write_audit(
                "位点归档", entry.get("数据库实例", ""),
                f"位点 {entry_id} 已标记为历史",
                "position_store", "已记录"
            )
        return entry, msg