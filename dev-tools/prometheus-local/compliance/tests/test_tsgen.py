#  Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
#  or more contributor license agreements. Licensed under the "Elastic License
#  2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
#  Public License v 1"; you may not use this file except in compliance with, at
#  your election, the "Elastic License 2.0", the "GNU Affero General Public
#  License v3.0 only", or the "Server Side Public License, v 1".

from tsgen import TSGen, _cartesian, _combo_salt, synth


# ---------------------------------------------------------------------------
# _cartesian
# ---------------------------------------------------------------------------


def test_cartesian_empty_yields_one_empty_combo():
    # A metric with no labels still emits exactly one series (with just the
    # COMMON_LABELS in TSGen).
    assert _cartesian({}) == [{}]


def test_cartesian_single_label_pairs_each_value():
    assert _cartesian({"a": ["x", "y"]}) == [{"a": "x"}, {"a": "y"}]


def test_cartesian_multiple_labels_full_product():
    assert _cartesian({"a": ["1", "2"], "b": ["x", "y"]}) == [
        {"a": "1", "b": "x"},
        {"a": "1", "b": "y"},
        {"a": "2", "b": "x"},
        {"a": "2", "b": "y"},
    ]


def test_cartesian_label_order_is_deterministic():
    # The output ordering is sorted by label name then by value, regardless
    # of input dict iteration order.
    a = _cartesian({"b": ["y"], "a": ["x"]})
    b = _cartesian({"a": ["x"], "b": ["y"]})
    assert a == b


# ---------------------------------------------------------------------------
# _combo_salt
# ---------------------------------------------------------------------------


def test_combo_salt_empty_combo_yields_empty_salt():
    assert _combo_salt({}) == ""


def test_combo_salt_sorts_labels_for_stability():
    # Order-independent so the same combo always produces the same RNG seed.
    assert _combo_salt({"b": "y", "a": "x"}) == "|a=x,b=y"


# ---------------------------------------------------------------------------
# TSGen.generate
# ---------------------------------------------------------------------------


def _gen(dimensions: dict, *, granularity_sec: int = 15, window_sec: int = 60):
    return TSGen(
        dimensions=dimensions,
        granularity_sec=granularity_sec,
        window_sec=window_sec,
        seed=42,
    ).generate(now_ms=1_700_000_000_000)


def test_generate_emits_one_series_per_metric_when_no_labels():
    out = _gen({"up": {}})
    assert len(out) == 1
    labels, samples = out[0]
    assert labels["__name__"] == "up"
    assert "job" in labels and "instance" in labels  # COMMON_LABELS
    assert len(samples) == 60 // 15


def test_generate_takes_cartesian_product_per_metric():
    out = _gen({"m": {"mode": ["a", "b"], "cpu": ["0", "1"]}})
    assert len(out) == 4


def test_generate_lets_query_label_override_common_label():
    out = _gen({"up": {"instance": ["override"]}})
    labels, _ = out[0]
    assert labels["instance"] == "override"


def test_generate_is_deterministic():
    a = _gen({"m": {"a": ["1", "2"]}})
    b = _gen({"m": {"a": ["1", "2"]}})
    assert a == b


def test_generate_distinct_combos_get_distinct_values():
    # Without `_combo_salt`, both combos would share the same RNG seed and
    # produce identical samples - which would make queries that filter on
    # `a` indistinguishable.
    out = _gen({"m": {"a": ["1", "2"]}})
    a_values = [v for v, _ in out[0][1]]
    b_values = [v for v, _ in out[1][1]]
    assert a_values != b_values


def test_generate_no_dimensions_produces_no_series():
    assert _gen({}) == []


# ---------------------------------------------------------------------------
# synth
# ---------------------------------------------------------------------------


def test_synth_counter_metric_is_strictly_increasing():
    timestamps = list(range(0, 60_000, 15_000))
    samples = synth(
        "node_cpu_seconds_total",  # `_total` suffix → counter
        timestamps,
        start_ms=0,
        period=30.0,
        seed=42,
    )
    values = [v for v, _ in samples]
    assert all(values[i] < values[i + 1] for i in range(len(values) - 1))


def test_synth_gauge_oscillates():
    # A gauge over a full period must NOT be monotonic; if it is we've
    # accidentally regenerated counter behaviour.
    timestamps = list(range(0, 600_000, 15_000))
    samples = synth(
        "node_load1",
        timestamps,
        start_ms=0,
        period=300.0,
        seed=42,
    )
    values = [v for v, _ in samples]
    monotonic_up = all(values[i] <= values[i + 1] for i in range(len(values) - 1))
    monotonic_down = all(values[i] >= values[i + 1] for i in range(len(values) - 1))
    assert not (monotonic_up or monotonic_down)


def test_synth_salt_changes_output():
    timestamps = list(range(0, 60_000, 15_000))
    a = synth("metric", timestamps, start_ms=0, period=30.0, seed=42, salt="|a=1")
    b = synth("metric", timestamps, start_ms=0, period=30.0, seed=42, salt="|a=2")
    assert a != b
