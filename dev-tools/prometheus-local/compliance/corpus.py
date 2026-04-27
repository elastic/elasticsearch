"""PromQL corpus loading, metric-name extraction, and synthetic data
generation. Pure parsing/computation - no IO except reading the corpus CSV
from disk."""

from __future__ import annotations

import csv
import math
import random
import re
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Protocol

from util import (
    COMMON_LABELS,
    COUNTER_SUFFIXES,
    CorpusStats,
    GRAFANA_VAR_SUBS,
    METRIC_NAME_RE,
    PROMQL_KEYWORDS,
    QueryCase,
    SAMPLE_INTERVAL_SECONDS,
    UNKNOWN_VAR_PLACEHOLDER,
    UNSUBSTITUTED_VAR_RE,
    UNSUPPORTED_FUNCTIONS,
)


# ---------------------------------------------------------------------------
# Metric name extraction from PromQL expressions
# ---------------------------------------------------------------------------


class MetricExtractor:
    @staticmethod
    def strip_for_metric_extraction(query: str) -> str:
        cleaned = re.sub(r'"[^"]*"', "", query)
        cleaned = re.sub(r"'[^']*'", "", cleaned)
        cleaned = re.sub(r"\{[^}]*\}", "", cleaned)
        cleaned = re.sub(r"\[[^\]]*\]", "", cleaned)

        grouping_keywords = (
            "by",
            "without",
            "on",
            "ignoring",
            "group_left",
            "group_right",
        )
        for keyword in grouping_keywords:
            cleaned = re.sub(rf"\b{keyword}\s*\([^)]*\)", " ", cleaned)

        return cleaned

    @classmethod
    def extract_metric_names(cls, query: str) -> set[str]:
        cleaned = cls.strip_for_metric_extraction(query)
        names: set[str] = set()

        for match in METRIC_NAME_RE.finditer(cleaned):
            name = match.group(1)

            if name in PROMQL_KEYWORDS or name.isdigit():
                continue

            rest = cleaned[match.end() :].lstrip()
            if rest.startswith("("):
                continue

            names.add(name)

        return names


# ---------------------------------------------------------------------------
# Corpus loaders
# ---------------------------------------------------------------------------


class CorpusLoader(Protocol):
    """Loads a corpus of PromQL queries from a CSV file."""

    def load(
        self,
        csv_path: Path,
        *,
        range_seconds: int,
        step_seconds: int,
    ) -> tuple[list[QueryCase], CorpusStats]: ...


class GrafanaCorpusLoader:
    """Loads queries from a Grafana-dashboards-analysis CSV
    (semicolon-delimited, with a "PromQL Query" column)."""

    @staticmethod
    def has_unsupported_function(query: str) -> bool:
        return any(
            re.search(rf"\b{re.escape(function)}\s*\(", query)
            for function in UNSUPPORTED_FUNCTIONS
        )

    @staticmethod
    def substitute_unknown_vars_in_strings(query: str) -> str:
        out: list[str] = []
        i = 0

        while i < len(query):
            char = query[i]

            if char in ('"', "'"):
                end = query.find(char, i + 1)
                if end == -1:
                    out.append(query[i:])
                    break

                inside = re.sub(
                    r"\$\{?\w+\}?",
                    UNKNOWN_VAR_PLACEHOLDER,
                    query[i + 1 : end],
                )
                out.append(char + inside + char)
                i = end + 1
                continue

            out.append(char)
            i += 1

        return "".join(out)

    @classmethod
    def substitute_grafana_vars(cls, query: str) -> str:
        for var, value in GRAFANA_VAR_SUBS.items():
            braced = "${" + var[1:] + "}"
            query = query.replace(braced, value)

        for var, value in GRAFANA_VAR_SUBS.items():
            query = query.replace(var, value)

        return cls.substitute_unknown_vars_in_strings(query)

    def load(
        self,
        csv_path: Path,
        *,
        range_seconds: int,
        step_seconds: int,
    ) -> tuple[list[QueryCase], CorpusStats]:
        cases: list[QueryCase] = []
        stats = CorpusStats()
        seen: set[str] = set()

        with csv_path.open(newline="") as file:
            reader = csv.DictReader(file, delimiter=";")

            for index, row in enumerate(reader):
                raw = (row.get("PromQL Query") or "").strip()

                if not raw:
                    stats = stats.increment("blank")
                    continue

                if self.has_unsupported_function(raw):
                    stats = stats.increment("unsupported")
                    continue

                substituted = self.substitute_grafana_vars(raw)

                if UNSUBSTITUTED_VAR_RE.search(substituted):
                    stats = stats.increment("unsubstitutable")
                    continue

                if substituted in seen:
                    stats = stats.increment("duplicate")
                    continue

                seen.add(substituted)
                cases.append(
                    QueryCase(
                        name=f"grafana_{index:04d}",
                        expr=substituted,
                        range_seconds=range_seconds,
                        step_seconds=step_seconds,
                        notes=raw[:120] + ("..." if len(raw) > 120 else ""),
                    )
                )
                stats = stats.increment("loaded")

        return cases, stats


# ---------------------------------------------------------------------------
# Synthetic data generation (deterministic given seed + samples + name)
# ---------------------------------------------------------------------------


class SyntheticDataFactory:
    @staticmethod
    def is_counter_name(name: str) -> bool:
        return name.endswith(COUNTER_SUFFIXES)

    @classmethod
    def synthesize_samples(
        cls,
        name: str,
        timestamps: Sequence[int],
        *,
        start_ms: int,
        period: float,
        seed: int,
    ) -> list[tuple[float, int]]:
        rnd = random.Random(f"{seed}:{name}")
        offset = rnd.random()

        if cls.is_counter_name(name):
            value = 0.0
            samples: list[tuple[float, int]] = []
            for index, ts_ms in enumerate(timestamps):
                value += 1.0 + (index % 5) * 0.1 + offset
                samples.append((round(value, 6), ts_ms))
            return samples

        amplitude = 25.0 + 50.0 * offset
        base = 50.0 + 100.0 * offset

        samples = []
        for ts_ms in timestamps:
            t_sec = (ts_ms - start_ms) / 1000.0
            value = base + amplitude * math.sin(
                2.0 * math.pi * t_sec / period + offset * math.pi
            )
            samples.append((round(value, 6), ts_ms))

        return samples

    @classmethod
    def generate_timeseries(
        cls,
        *,
        now_ms: int,
        metric_names: Iterable[str],
        samples: int,
        seed: int,
    ) -> list[tuple[dict[str, str], list[tuple[float, int]]]]:
        interval_ms = SAMPLE_INTERVAL_SECONDS * 1000
        end_ms = (now_ms // interval_ms) * interval_ms
        start_ms = end_ms - (samples - 1) * interval_ms
        timestamps = list(range(start_ms, end_ms + interval_ms, interval_ms))
        period = max(1.0, (samples - 1) * SAMPLE_INTERVAL_SECONDS / 2.0)

        return [
            (
                {"__name__": name, **COMMON_LABELS},
                cls.synthesize_samples(
                    name,
                    timestamps,
                    start_ms=start_ms,
                    period=period,
                    seed=seed,
                ),
            )
            for name in sorted(set(metric_names))
        ]
