#  Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
#  or more contributor license agreements. Licensed under the "Elastic License
#  2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
#  Public License v 1"; you may not use this file except in compliance with, at
#  your election, the "Elastic License 2.0", the "GNU Affero General Public
#  License v3.0 only", or the "Server Side Public License, v 1".

"""Tests for the CLI-side helpers that are pure functions: the regex and
granularity ParamTypes, and case filtering. `ParamType.convert` takes
`(value, param, ctx)`; we pass `None` for param/ctx since neither is read
on the happy path or when raising."""

import re

import click
import pytest

from corpus import QueryCase
from main import CHRONO, REGEX, filter_cases, matches_err_filter


# ---------------------------------------------------------------------------
# RegexType
# ---------------------------------------------------------------------------


def test_regex_valid_returns_pattern():
    result = REGEX.convert(r"\bfoo\b", None, None)
    assert isinstance(result, re.Pattern)
    assert result.search("foo bar")
    assert not result.search("foobar")


def test_regex_passthrough_for_already_compiled_pattern():
    pattern = re.compile(r"foo")
    assert REGEX.convert(pattern, None, None) is pattern


def test_regex_invalid_raises_bad_parameter():
    with pytest.raises(click.BadParameter, match="invalid regex"):
        REGEX.convert("[unclosed", None, None)


# ---------------------------------------------------------------------------
# GranularityType
# ---------------------------------------------------------------------------


def test_granularity_seconds():
    assert CHRONO.convert("15s", None, None) == 15


def test_granularity_minutes():
    assert CHRONO.convert("1m", None, None) == 60


def test_granularity_passthrough_for_already_converted_int():
    # Defensive: Click only feeds in str at parse time, but if a caller
    # somehow re-runs convert on the already-converted value we shouldn't
    # crash trying to regex-match an int.
    assert CHRONO.convert(15, None, None) == 15


def test_granularity_rejects_missing_unit():
    with pytest.raises(click.BadParameter, match="invalid duration"):
        CHRONO.convert("15", None, None)


def test_granularity_rejects_unknown_unit():
    with pytest.raises(click.BadParameter, match="invalid duration"):
        CHRONO.convert("15d", None, None)


def test_granularity_rejects_zero():
    with pytest.raises(click.BadParameter):
        CHRONO.convert("0s", None, None)


def test_granularity_rejects_too_coarse_for_min_samples():
    # SEED_WINDOW_SECONDS is 1800, MIN_SAMPLES_PER_SERIES is 4. 10m → 600s
    # gives 1800//600 = 3 samples, which is below the floor.
    with pytest.raises(click.BadParameter, match=r"need >="):
        CHRONO.convert("10m", None, None)


def test_granularity_recognizes_hours_unit():
    # No `Nh` value passes the floor (1800 // 3600 = 0), but reaching the
    # floor check at all proves `h` is a recognized unit; otherwise we'd hit
    # "invalid duration" first.
    with pytest.raises(click.BadParameter, match=r"need >="):
        CHRONO.convert("1h", None, None)


# ---------------------------------------------------------------------------
# filter_cases
# ---------------------------------------------------------------------------


def _case(expr: str) -> QueryCase:
    return QueryCase(name="t", expr=expr, range_seconds=60, step_seconds=15)


def test_filter_cases_returns_all_when_no_pattern():
    cases = [_case("a"), _case("b")]
    assert filter_cases(cases, None) == cases


def test_filter_cases_keeps_only_matching():
    cases = [_case("foo bar"), _case("baz")]
    result = filter_cases(cases, re.compile(r"foo"))
    assert [c.expr for c in result] == ["foo bar"]


def test_filter_cases_with_no_matches_raises_click_exception():
    with pytest.raises(click.ClickException, match="no queries match"):
        filter_cases([_case("a")], re.compile(r"xyz"))


# ---------------------------------------------------------------------------
# matches_err_filter
# ---------------------------------------------------------------------------


def test_err_filter_matches_against_lowercased_error():
    # The backend may emit `Function [foo] is not yet implemented` (capital
    # F); the user writes `function ...` and it must still match.
    pattern = re.compile(r"function \[[^]]+\] is not yet implemented")
    assert matches_err_filter("Function [foo] is not yet implemented", pattern)


def test_err_filter_pattern_with_uppercase_does_not_match_lowercased_error():
    # The contract goes one way: errors are lowercased, patterns aren't.
    # An uppercase pattern won't find anything because the input is folded
    # to lowercase first - this is intentional, so users always write
    # lowercase patterns.
    pattern = re.compile(r"FUNCTION")
    assert not matches_err_filter("Function [x]", pattern)


def test_err_filter_returns_false_for_no_pattern():
    assert not matches_err_filter("any error", None)


def test_err_filter_returns_false_for_no_error():
    pattern = re.compile(r"anything")
    assert not matches_err_filter(None, pattern)
    assert not matches_err_filter("", pattern)
