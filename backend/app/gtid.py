"""GTID 区间归并引擎：按事务来源归并 GTID 区间，消除重复计数。

位点存储与故障切换共用本模块：
- 同一来源的重叠/相邻区间只计一次（如 orders:1-5 与 orders:4-8 归并为 orders:1-8）；
- 不同来源的相同编号事务分别计算（orders:1 与 inventory:1 是两笔事务）；
- 支持期望区间减去已确认区间得到缺口区间，作为切换恢复判定的依据。
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Iterable

# 兼容 "orders:3"、"orders:1-5" 两种写法，来源标签允许字母数字与 _ . -
_PART_RE = re.compile(r"^\s*([A-Za-z0-9_][\w.-]*):(\d+)(?:-(\d+))?\s*$")


@dataclass(frozen=True)
class GtidInterval:
    """单个事务来源的一段闭区间 GTID。"""

    source: str
    start: int
    end: int

    def __post_init__(self) -> None:
        if self.end < self.start:
            raise ValueError(f"GTID 区间终点小于起点: {self.source}:{self.start}-{self.end}")

    @property
    def count(self) -> int:
        return self.end - self.start + 1

    def contains(self, n: int) -> bool:
        return self.start <= n <= self.end

    def label(self) -> str:
        return f"{self.source}:{self.start}" if self.start == self.end else f"{self.source}:{self.start}-{self.end}"

    def to_dict(self) -> dict[str, Any]:
        return {"source": self.source, "start": self.start, "end": self.end, "count": self.count}


def parse_gtid_intervals(text: str) -> list[GtidInterval]:
    """解析位点存储中的 GTID 集合字符串。

    支持逗号/分号分隔的混合写法，例如 "orders:1-5,orders:4-8;inventory:1-3"。
    解析结果保留原始区间（不去重），归并交给 merge_intervals。
    """
    intervals: list[GtidInterval] = []
    for part in re.split(r"[,;]", text or ""):
        part = part.strip()
        if not part:
            continue
        match = _PART_RE.match(part)
        if not match:
            continue
        start = int(match.group(2))
        end = int(match.group(3)) if match.group(3) else start
        intervals.append(GtidInterval(match.group(1), start, end))
    return intervals


def format_gtid_intervals(intervals: Iterable[GtidInterval]) -> str:
    """把区间列表格式化为规范 GTID 集合字符串（按来源、起点排序，保留原始重叠）。"""
    return ",".join(iv.label() for iv in sorted(intervals, key=lambda i: (i.source, i.start, i.end)))


def merge_intervals(intervals: Iterable[GtidInterval]) -> dict[str, list[GtidInterval]]:
    """按来源归并区间：重叠或相邻的区间合并，同一来源的重叠区间只计一次。"""
    by_source: dict[str, list[GtidInterval]] = {}
    for iv in intervals:
        by_source.setdefault(iv.source, []).append(iv)
    merged: dict[str, list[GtidInterval]] = {}
    for source, ivs in by_source.items():
        ordered = sorted(ivs, key=lambda i: (i.start, i.end))
        bucket: list[GtidInterval] = []
        cur_start, cur_end = ordered[0].start, ordered[0].end
        for iv in ordered[1:]:
            if iv.start <= cur_end + 1:  # 重叠或相邻
                cur_end = max(cur_end, iv.end)
            else:
                bucket.append(GtidInterval(source, cur_start, cur_end))
                cur_start, cur_end = iv.start, iv.end
        bucket.append(GtidInterval(source, cur_start, cur_end))
        merged[source] = bucket
    return merged


def count_by_source(merged: dict[str, list[GtidInterval]]) -> dict[str, int]:
    """各来源去重后的事务数。"""
    return {src: sum(iv.count for iv in ivs) for src, ivs in merged.items()}


def total_count(merged: dict[str, list[GtidInterval]]) -> int:
    """跨来源合计：异源同编号分别计算（按来源分别计数再相加）。"""
    return sum(iv.count for ivs in merged.values() for iv in ivs)


def naive_count(intervals: Iterable[GtidInterval]) -> int:
    """未归并的原始计数（旧逻辑）：同一来源的重叠区间会被重复计入。"""
    return sum(iv.count for iv in intervals)


def subtract(
    expected: dict[str, list[GtidInterval]],
    confirmed: dict[str, list[GtidInterval]],
) -> dict[str, list[GtidInterval]]:
    """期望覆盖减去已确认覆盖，得到各来源的缺口区间。"""
    gaps: dict[str, list[GtidInterval]] = {}
    for source, exp_ivs in expected.items():
        conf_ivs = confirmed.get(source, [])
        remaining: list[GtidInterval] = []
        for exp in exp_ivs:
            segments = [(exp.start, exp.end)]
            for conf in conf_ivs:
                next_segments: list[tuple[int, int]] = []
                for seg_start, seg_end in segments:
                    if conf.end < seg_start or conf.start > seg_end:
                        next_segments.append((seg_start, seg_end))
                        continue
                    if seg_start < conf.start:
                        next_segments.append((seg_start, conf.start - 1))
                    if conf.end < seg_end:
                        next_segments.append((conf.end + 1, seg_end))
                segments = next_segments
            remaining.extend(GtidInterval(source, s, e) for s, e in segments)
        if remaining:
            gaps[source] = remaining
    return gaps


def covers(merged: dict[str, list[GtidInterval]], source: str, n: int) -> bool:
    """判断某来源的第 n 笔事务是否已被覆盖。"""
    return any(iv.contains(n) for iv in merged.get(source, []))


def expand(merged: dict[str, list[GtidInterval]], source: str) -> list[int]:
    """展开某来源的归并区间为有序事务编号列表。"""
    out: list[int] = []
    for iv in merged.get(source, []):
        out.extend(range(iv.start, iv.end + 1))
    return sorted(out)
