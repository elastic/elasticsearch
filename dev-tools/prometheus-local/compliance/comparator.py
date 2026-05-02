#  Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
#  or more contributor license agreements. Licensed under the "Elastic License
#  2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
#  Public License v 1"; you may not use this file except in compliance with, at
#  your election, the "Elastic License 2.0", the "GNU Affero General Public
#  License v3.0 only", or the "Server Side Public License, v 1".

from __future__ import annotations

import asyncio
import json
import math
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

import httpx

from corpus import QueryCase
from proc import Proc
from util import (
    DEFAULT_FAILURE_CAP,
    HttpError,
    PointDiff,
    SeriesIssue,
)


# Floating-point tolerances for value comparison. Same code path on
# control and test can produce values that differ by a few ulps (e.g.
# `0.14766520000000003` vs `0.14766520000000001`, abs diff ~3e-17) when
# operations happen in different orders or in different libraries.
# `rel_tol=1e-9` is roughly 8 decimal digits, which is far beyond what
# Prometheus exposes via `query_range` and well above ulp noise; `abs_tol`
# catches the same situation when values are near zero where `rel_tol`
# degenerates.
_REL_TOL = 1e-9
_ABS_TOL = 1e-12


def _values_match(control_value: float, test_value: float) -> bool:
    """True if both values are NaN, or finite and equal within tolerance."""
    control_nan = math.isnan(control_value)
    test_nan = math.isnan(test_value)
    if control_nan and test_nan:
        return True
    if control_nan or test_nan:
        return False
    return math.isclose(
        control_value, test_value, rel_tol=_REL_TOL, abs_tol=_ABS_TOL
    )


# ---------------------------------------------------------------------------
# Diff + comparator
# ---------------------------------------------------------------------------


@dataclass
class CompareResult:
    case: QueryCase
    start_sec: float
    end_sec: float
    ok: bool = False
    series_count: int = 0
    point_count: int = 0
    failure_count: int = 0
    max_abs_diff: float = 0.0
    series_issues: list[SeriesIssue] = field(default_factory=list)
    failures: list[PointDiff] = field(default_factory=list)
    control_latency_ms: float = 0.0
    test_latency_ms: float = 0.0
    error: str | None = None
    skipped: bool = False

    @property
    def status(self) -> str:
        if self.skipped:
            return "SKIP"
        if self.error is not None:
            return "ERROR"
        return "OK" if self.ok else "FAIL"


class Comparator:
    """
    Runs a single PromQL `query_range` against both
    test and control folds the result diff into a `Diff`.
    """

    def __init__(
        self,
        *,
        control_proc: Proc,
        test_proc: Proc,
        failure_cap: int = DEFAULT_FAILURE_CAP,
    ) -> None:
        self.control_proc = control_proc
        self.test_proc = test_proc
        self.failure_cap = failure_cap

    async def run(self, case: QueryCase, *, end_sec: float) -> CompareResult:
        start_sec = end_sec - case.range_seconds
        comparison = CompareResult(case=case, start_sec=start_sec, end_sec=end_sec)

        try:
            (
                (control_result, comparison.control_latency_ms),
                (
                    test_result,
                    comparison.test_latency_ms,
                ),
            ) = await asyncio.gather(
                self.control_proc.query_range(
                    query=case.expr,
                    start=start_sec,
                    end=end_sec,
                    step=case.step_seconds,
                ),
                self.test_proc.query_range(
                    query=case.expr,
                    start=start_sec,
                    end=end_sec,
                    step=case.step_seconds,
                ),
            )
        except (HttpError, httpx.HTTPError, ValueError) as exc:
            comparison.error = str(exc)
            return comparison

        self._compare_into(control_result, test_result, comparison)
        return comparison

    @staticmethod
    def _normalize(
        result: Mapping[str, Any],
    ) -> dict[str, list[tuple[float, float]]]:
        """Index series by canonical label key. __name__ is stripped because
        it is dropped from function results on the control side (Prometheus)
        but may be retained on the test side (Elasticsearch)."""
        output: dict[str, list[tuple[float, float]]] = {}

        for series in result.get("data", {}).get("result", []):
            labels = {
                key: value
                for key, value in series.get("metric", {}).items()
                if key != "__name__"
            }
            label_key = json.dumps(labels, sort_keys=True)
            output[label_key] = [
                (float(timestamp), float(value))
                for timestamp, value in series.get("values", [])
            ]

        return output

    def _compare_into(
        self,
        control: Mapping[str, Any],
        test: Mapping[str, Any],
        comparison: CompareResult,
    ) -> None:
        control_norm = self._normalize(control)
        test_norm = self._normalize(test)

        control_keys = set(control_norm)
        test_keys = set(test_norm)

        only_control = sorted(control_keys - test_keys)
        only_test = sorted(test_keys - control_keys)

        if only_control:
            comparison.series_issues.append(
                SeriesIssue(
                    f"only in control ({len(only_control)}): {only_control[:3]}"
                )
            )

        if only_test:
            comparison.series_issues.append(
                SeriesIssue(f"only in test    ({len(only_test)}): {only_test[:3]}")
            )

        common_keys = sorted(control_keys & test_keys)
        comparison.series_count = len(common_keys)

        for key in common_keys:
            control_points = dict(control_norm[key])
            test_points = dict(test_norm[key])

            control_timestamps = set(control_points)
            test_timestamps = set(test_points)

            only_control_ts = sorted(control_timestamps - test_timestamps)
            only_test_ts = sorted(test_timestamps - control_timestamps)

            if only_control_ts:
                comparison.series_issues.append(
                    SeriesIssue(
                        f"{key}: {len(only_control_ts)} ts only in control "
                        f"(e.g. {only_control_ts[:3]})"
                    )
                )

            if only_test_ts:
                comparison.series_issues.append(
                    SeriesIssue(
                        f"{key}: {len(only_test_ts)} ts only in test "
                        f"(e.g. {only_test_ts[:3]})"
                    )
                )

            for timestamp in sorted(control_timestamps & test_timestamps):
                comparison.point_count += 1

                control_value = control_points[timestamp]
                test_value = test_points[timestamp]

                if _values_match(control_value, test_value):
                    continue

                comparison.failure_count += 1

                if not (math.isnan(control_value) or math.isnan(test_value)):
                    abs_diff = abs(control_value - test_value)
                    comparison.max_abs_diff = max(comparison.max_abs_diff, abs_diff)
                else:
                    abs_diff = math.nan

                if len(comparison.failures) < self.failure_cap:
                    comparison.failures.append(
                        PointDiff(timestamp, control_value, test_value, abs_diff)
                    )

        comparison.ok = not comparison.series_issues and comparison.failure_count == 0
