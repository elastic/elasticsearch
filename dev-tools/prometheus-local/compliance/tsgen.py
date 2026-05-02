#  Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
#  or more contributor license agreements. Licensed under the "Elastic License
#  2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
#  Public License v 1"; you may not use this file except in compliance with, at
#  your election, the "Elastic License 2.0", the "GNU Affero General Public
#  License v3.0 only", or the "Server Side Public License, v 1".

from __future__ import annotations

import itertools
import math
import random
from collections.abc import Iterable, Mapping, Sequence

from util import (
    COMMON_LABELS,
    COUNTER_SUFFIXES,
)


# ---------------------------------------------------------------------------
# Deterministic synthetic timeseries data generator
# ---------------------------------------------------------------------------


class TSGen:
    """Generates one synthetic series per `(metric, label-combo)` pair.

    Dimensions are passed as `{metric: {label: {values}}}`. Each metric's
    series set is the cartesian product of its label-value sets, joined with
    `COMMON_LABELS`. Metrics with no recorded labels get a single series
    bearing only `COMMON_LABELS`. Output is deterministic given
    `(seed, metric, label-combo)`.
    """

    def __init__(
        self,
        *,
        dimensions: Mapping[str, Mapping[str, Iterable[str]]],
        granularity_sec: int,
        window_sec: int,
        seed: int,
    ) -> None:
        self._dimensions = {
            metric: {
                label: tuple(sorted(set(values))) for label, values in labels.items()
            }
            for metric, labels in dimensions.items()
        }
        self._granularity_sec = granularity_sec
        self._window_sec = window_sec
        self._seed = seed

    def generate(
        self, now_ms: int
    ) -> list[tuple[dict[str, str], list[tuple[float, int]]]]:
        interval_ms = self._granularity_sec * 1000
        samples = self._window_sec // self._granularity_sec
        end_ms = (now_ms // interval_ms) * interval_ms
        start_ms = end_ms - (samples - 1) * interval_ms
        timestamps = list(range(start_ms, end_ms + interval_ms, interval_ms))
        period = max(1.0, (samples - 1) * self._granularity_sec / 2.0)

        out: list[tuple[dict[str, str], list[tuple[float, int]]]] = []
        for metric in sorted(self._dimensions):
            for combo in _cartesian(self._dimensions[metric]):
                labels = {"__name__": metric, **COMMON_LABELS, **combo}
                out.append(
                    (
                        labels,
                        synth(
                            metric,
                            timestamps,
                            start_ms=start_ms,
                            period=period,
                            seed=self._seed,
                            salt=_combo_salt(combo),
                        ),
                    )
                )
        return out


def _cartesian(
    label_values: Mapping[str, Sequence[str]],
) -> list[dict[str, str]]:
    """Cartesian product of label values. Returns `[{}]` for an empty input
    so a metric with no labels still produces exactly one series."""
    if not label_values:
        return [{}]

    items = sorted(label_values.items())
    keys = [k for k, _ in items]
    return [
        dict(zip(keys, combo))
        for combo in itertools.product(*(values for _, values in items))
    ]


def _combo_salt(combo: Mapping[str, str]) -> str:
    if not combo:
        return ""
    return "|" + ",".join(f"{k}={v}" for k, v in sorted(combo.items()))


def synth(
    metric: str,
    timestamps: Sequence[int],
    *,
    start_ms: int,
    period: float,
    seed: int,
    salt: str = "",
) -> list[tuple[float, int]]:
    """Synthesize one series. `salt` lets two series of the same metric (with
    different label combos) get distinct values while staying deterministic."""
    rnd = random.Random(f"{seed}:{metric}{salt}")
    offset = rnd.random()

    if metric.endswith(COUNTER_SUFFIXES):
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
