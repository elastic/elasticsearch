#  Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
#  or more contributor license agreements. Licensed under the "Elastic License
#  2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
#  Public License v 1"; you may not use this file except in compliance with, at
#  your election, the "Elastic License 2.0", the "GNU Affero General Public
#  License v3.0 only", or the "Server Side Public License, v 1".

from pathlib import Path

from corpus import (
    CorpusStats,
    DimensionExtractor,
    GrafanaDashboardsQueryCorpusLoader,
    extract_metric_names,
)


# ---------------------------------------------------------------------------
# extract_metric_names
# ---------------------------------------------------------------------------


def test_metric_extractor_finds_bare_name():
    assert extract_metric_names("up") == {"up"}


def test_metric_extractor_finds_name_with_selector():
    assert extract_metric_names('up{job="x"}') == {"up"}


def test_metric_extractor_skips_promql_keywords():
    # `sum` is a PromQL aggregator; only `up` is the metric.
    assert extract_metric_names("sum(up)") == {"up"}


def test_metric_extractor_skips_function_calls():
    # `rate` is a function; the inner `node_cpu` is the metric.
    assert extract_metric_names("rate(node_cpu[5m])") == {"node_cpu"}


def test_metric_extractor_finds_multiple_metrics_in_arithmetic():
    assert extract_metric_names("a + b") == {"a", "b"}


# ---------------------------------------------------------------------------
# DimensionExtractor
# ---------------------------------------------------------------------------


def test_dimension_extractor_captures_simple_equality():
    assert DimensionExtractor.extract('m{a="x"}') == {"m": {"a": {"x"}}}


def test_dimension_extractor_captures_multiple_labels_one_metric():
    assert DimensionExtractor.extract('m{a="x", b="y"}') == {
        "m": {"a": {"x"}, "b": {"y"}}
    }


def test_dimension_extractor_ignores_regex_matchers():
    # `=~` cannot be enumerated, so we drop it. The metric is still recorded.
    assert DimensionExtractor.extract('m{a=~"x"}') == {"m": {}}


def test_dimension_extractor_ignores_negation_matchers():
    assert DimensionExtractor.extract('m{a!="x"}') == {"m": {}}


def test_dimension_extractor_records_metric_with_empty_selector():
    assert DimensionExtractor.extract("m{}") == {"m": {}}


def test_dimension_extractor_keeps_unknown_placeholder_value():
    # `__unknown__` is the placeholder left behind by `util.substitute` for
    # unsubstituted variables in string literals. We deliberately keep it so
    # seeded data still matches the substituted query.
    assert DimensionExtractor.extract('m{a="__unknown__"}') == {
        "m": {"a": {"__unknown__"}}
    }


def test_dimension_extractor_skips_promql_keyword_as_name():
    # `sum{a="x"}` is malformed PromQL but defensively the extractor must
    # not invent a series called `sum`.
    assert DimensionExtractor.extract('sum{a="x"}') == {}


def test_dimension_extractor_unions_label_values_within_one_query():
    assert DimensionExtractor.extract('m{a="1"} + m{a="2"}') == {"m": {"a": {"1", "2"}}}


# ---------------------------------------------------------------------------
# CorpusStats._record
# ---------------------------------------------------------------------------


def test_record_registers_bare_metric_names_with_empty_label_dicts():
    stats = CorpusStats()
    stats._record("up + node_load1")
    assert stats.dimensions == {"up": {}, "node_load1": {}}


def test_record_merges_label_values_across_multiple_calls():
    stats = CorpusStats()
    stats._record('m{a="1"}')
    stats._record('m{a="2", b="x"}')
    assert stats.dimensions == {"m": {"a": {"1", "2"}, "b": {"x"}}}


def test_record_keeps_metric_when_seen_with_and_without_selectors():
    stats = CorpusStats()
    stats._record('m{a="1"}')
    stats._record("m + 1")
    assert stats.dimensions == {"m": {"a": {"1"}}}


def test_corpus_stats_repr_includes_metric_count():
    stats = CorpusStats(total=10, skipped=2)
    stats._record('m{a="1"}')
    text = repr(stats)
    assert "total=10" in text
    assert "skipped=2" in text
    assert "metrics=1" in text


# ---------------------------------------------------------------------------
# GrafanaDashboardsQueryCorpusLoader
# ---------------------------------------------------------------------------


def _write_csv(tmp_path: Path, body: str) -> Path:
    p = tmp_path / "corpus.csv"
    p.write_text("PromQL Query\n" + body)
    return p


def test_loader_reads_csv_and_populates_dimensions(tmp_path):
    p = _write_csv(
        tmp_path,
        'up{job="x"}\nsum(rate(node_cpu_total[5m]))\n',
    )

    cases, stats = GrafanaDashboardsQueryCorpusLoader().load(
        p, range_seconds=600, step_seconds=30
    )

    assert len(cases) == 2
    assert stats.total == 2
    assert stats.skipped == 0
    assert stats.dimensions["up"] == {"job": {"x"}}
    # Bare metric inside `rate(...)` registered with no label values.
    assert stats.dimensions["node_cpu_total"] == {}


def test_loader_skips_whitespace_only_rows(tmp_path):
    # `csv.DictReader` swallows truly blank lines (they never reach the
    # loader), but a whitespace-only row does. The loader counts it as
    # total-but-skipped.
    p = _write_csv(tmp_path, "   \nup\n")
    cases, stats = GrafanaDashboardsQueryCorpusLoader().load(
        p, range_seconds=600, step_seconds=30
    )
    assert len(cases) == 1
    assert stats.total == 2
    assert stats.skipped == 1


def test_loader_skips_unsubstitutable_queries(tmp_path):
    # `$totally_unknown` inside a string is not in `GRAFANA_VAR_SUBS`, so
    # `substitute()` returns ok=False and the loader drops the row.
    p = _write_csv(tmp_path, 'metric{a="$totally_unknown"}\n')
    cases, stats = GrafanaDashboardsQueryCorpusLoader().load(
        p, range_seconds=600, step_seconds=30
    )
    assert cases == []
    assert stats.skipped == 1
    assert stats.dimensions == {}


def test_loader_dedups_byte_identical_queries(tmp_path):
    p = _write_csv(tmp_path, "up\nup\n")
    cases, stats = GrafanaDashboardsQueryCorpusLoader().load(
        p, range_seconds=600, step_seconds=30
    )
    assert len(cases) == 1
    assert stats.total == 2
    assert stats.skipped == 1


def test_loader_records_dimensions_only_for_first_seen_duplicate(tmp_path):
    # Both rows are byte-identical; the second is skipped, but the corpus
    # still ends up with the right dimensions from the first.
    p = _write_csv(tmp_path, 'up{job="x"}\nup{job="x"}\n')
    cases, stats = GrafanaDashboardsQueryCorpusLoader().load(
        p, range_seconds=600, step_seconds=30
    )
    assert len(cases) == 1
    assert stats.dimensions == {"up": {"job": {"x"}}}
