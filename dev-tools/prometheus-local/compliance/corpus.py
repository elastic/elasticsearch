#  Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
#  or more contributor license agreements. Licensed under the "Elastic License
#  2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
#  Public License v 1"; you may not use this file except in compliance with, at
#  your election, the "Elastic License 2.0", the "GNU Affero General Public
#  License v3.0 only", or the "Server Side Public License, v 1".

from __future__ import annotations

import csv
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

from util import (
    GRAFANA_VAR_SUBS,
    METRIC_NAME_RE,
    PROMQL_KEYWORDS,
    substitute,
)


# ---------------------------------------------------------------------------
# Metric name extraction from PromQL expressions
# ---------------------------------------------------------------------------


_GROUPING_KEYWORDS = ("by", "without", "on", "ignoring", "group_left", "group_right")


def _strip_for_metric_extraction(query: str) -> str:
    cleaned = re.sub(r'"[^"]*"', "", query)
    cleaned = re.sub(r"'[^']*'", "", cleaned)
    cleaned = re.sub(r"\{[^}]*\}", "", cleaned)
    cleaned = re.sub(r"\[[^\]]*\]", "", cleaned)

    for keyword in _GROUPING_KEYWORDS:
        cleaned = re.sub(rf"\b{keyword}\s*\([^)]*\)", " ", cleaned)

    return cleaned


def extract_metric_names(query: str) -> set[str]:
    cleaned = _strip_for_metric_extraction(query)
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
# Dimension (label-value) extraction
# ---------------------------------------------------------------------------


# `name{...}` - capture the metric name plus the (possibly empty) selector
# block. The metric-name part purposefully avoids matching after `(` (those
# are function calls), but in PromQL a function never has a trailing `{...}`,
# so this regex never sees one.
_NAME_WITH_SELECTOR_RE = re.compile(r"\b([a-zA-Z_:][a-zA-Z0-9_:]*)\s*\{([^}]*)\}")

# Equality matcher inside a selector: `label = "value"`. Skips `!=`, `=~`, and
# `!~` because we can't enumerate matching values for those.
_LABEL_EQ_RE = re.compile(r'\b([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*"([^"]*)"')


class DimensionExtractor:
    """Per-metric label values seen in a PromQL expression.

    Returns `{metric: {label: {values}}}`. Only equality matchers (`=`) are
    captured; regex/negation matchers are ignored. Values are kept verbatim,
    including the `__unknown__` placeholder left over from
    `util.substitute()` for unsubstituted variables - so seeded data can
    still match those queries.
    """

    @staticmethod
    def extract(query: str) -> dict[str, dict[str, set[str]]]:
        out: dict[str, dict[str, set[str]]] = {}

        for match in _NAME_WITH_SELECTOR_RE.finditer(query):
            name = match.group(1)
            if name in PROMQL_KEYWORDS or name.isdigit():
                continue

            label_values = out.setdefault(name, {})
            for lv in _LABEL_EQ_RE.finditer(match.group(2)):
                label, value = lv.group(1), lv.group(2)
                label_values.setdefault(label, set()).add(value)

        return out


# ---------------------------------------------------------------------------
# Corpus loaders
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class QueryCase:
    name: str
    expr: str
    range_seconds: int
    step_seconds: int
    notes: str = ""


@dataclass
class CorpusStats:
    """Counts plus the dimensions seen across the loaded corpus.

    `dimensions` is `{metric: {label: {values}}}` aggregated over every
    accepted query: it captures the metric names referenced in the corpus
    AND the equality-matched label values seen for each. Downstream seeders
    (e.g. `TSGen`) use this to generate one synthetic series per
    `(metric, label-combo)` so queries that filter on those labels match
    real data instead of returning empty results.
    """

    total: int = 0
    skipped: int = 0
    dimensions: dict[str, dict[str, set[str]]] = field(default_factory=dict)

    def _add_total(self, val: int = 1) -> int:
        self.total += val
        return self.total

    def _add_skipped(self, val: int = 1) -> int:
        self.skipped += val
        return self.skipped

    def _record(self, query: str) -> None:
        """Fold a query's metric/label references into `dimensions`."""
        for name in extract_metric_names(query):
            self.dimensions.setdefault(name, {})

        for metric, label_values in DimensionExtractor.extract(query).items():
            bucket = self.dimensions.setdefault(metric, {})
            for label, values in label_values.items():
                bucket.setdefault(label, set()).update(values)

    def __repr__(self) -> str:
        return (
            f"CorpusStats(total={self.total} skipped={self.skipped} "
            f"metrics={len(self.dimensions)})"
        )


class CorpusLoader(Protocol):
    """Loads a corpus of PromQL queries"""

    def load(
        self,
        csv_path: Path,
        *,
        range_seconds: int,
        step_seconds: int,
    ) -> tuple[list[QueryCase], CorpusStats]: ...


class GrafanaDashboardsQueryCorpusLoader:
    _QUERY_COLUMN = "PromQL Query"

    def load(
        self,
        p: Path,
        *,
        range_seconds: int,
        step_seconds: int,
    ) -> tuple[list[QueryCase], CorpusStats]:
        stats = CorpusStats()
        cases: list[QueryCase] = []
        seen: set[str] = set()

        with p.open(newline="") as fd:
            reader = csv.DictReader(fd, delimiter=";")

            for index, row in enumerate(reader):
                raw = (row.get(self._QUERY_COLUMN) or "").strip()
                stats._add_total()

                if not raw:
                    stats._add_skipped()
                    continue

                ok, s = substitute(raw, GRAFANA_VAR_SUBS)

                if not ok:
                    stats._add_skipped()
                    continue

                if s in seen:
                    stats._add_skipped()
                    continue
                seen.add(s)

                cases.append(
                    QueryCase(
                        name=f"{index:03d}",
                        expr=s,
                        range_seconds=range_seconds,
                        step_seconds=step_seconds,
                        notes=raw,
                    )
                )
                stats._record(s)

        return cases, stats
