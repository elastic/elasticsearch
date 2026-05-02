#  Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
#  or more contributor license agreements. Licensed under the "Elastic License
#  2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
#  Public License v 1"; you may not use this file except in compliance with, at
#  your election, the "Elastic License 2.0", the "GNU Affero General Public
#  License v3.0 only", or the "Server Side Public License, v 1".

import math

from comparator import Comparator, CompareResult
from corpus import QueryCase


def _diff() -> CompareResult:
    return CompareResult(
        case=QueryCase(name="t", expr="up", range_seconds=60, step_seconds=15),
        start_sec=0,
        end_sec=60,
    )


def _comparator(failure_cap: int = 5) -> Comparator:
    """Build a Comparator without invoking __init__ (which requires
    real Procs we don't need for the pure compare/normalize logic)."""
    cmp = Comparator.__new__(Comparator)
    cmp.failure_cap = failure_cap
    return cmp


def _payload(*series: tuple[dict[str, str], list[tuple[float, str]]]) -> dict:
    return {
        "data": {
            "result": [
                {"metric": dict(metric), "values": [list(v) for v in values]}
                for metric, values in series
            ]
        }
    }


# ---------------------------------------------------------------------------
# _normalize
# ---------------------------------------------------------------------------


def test_normalize_strips_name_label():
    payload = _payload(
        ({"__name__": "up", "job": "x"}, [(0.0, "1")]),
    )
    result = Comparator._normalize(payload)
    # `__name__` is gone; only `job` is part of the label key.
    assert list(result.keys()) == ['{"job": "x"}']


def test_normalize_parses_values_to_floats():
    payload = _payload(
        ({"a": "1"}, [(1.0, "5"), (2.0, "5.5")]),
    )
    assert Comparator._normalize(payload) == {
        '{"a": "1"}': [(1.0, 5.0), (2.0, 5.5)],
    }


def test_normalize_empty_result_is_empty_dict():
    assert Comparator._normalize({"data": {"result": []}}) == {}


# ---------------------------------------------------------------------------
# _compare_into - happy path
# ---------------------------------------------------------------------------


def test_identical_payloads_yield_ok():
    cmp = _comparator()
    payload = _payload(({"a": "1"}, [(1.0, "5")]))
    diff = _diff()

    cmp._compare_into(payload, payload, diff)

    assert diff.ok is True
    assert diff.failure_count == 0
    assert diff.point_count == 1
    assert diff.series_count == 1


# ---------------------------------------------------------------------------
# _compare_into - value mismatches
# ---------------------------------------------------------------------------


def test_ulp_scale_difference_is_treated_as_match():
    # Same arithmetic done in different orders on control vs test can
    # produce values that differ by a few ulps. These must NOT be reported
    # as failures - the comparator uses a relative tolerance well above
    # ulp noise but tight enough to flag real divergence.
    cmp = _comparator()
    control = _payload(({"a": "1"}, [(1.0, "0.1476652")]))
    test = _payload(({"a": "1"}, [(1.0, "0.14766520000000003")]))  # +3 ulps
    diff = _diff()

    cmp._compare_into(control, test, diff)

    assert diff.ok is True
    assert diff.failure_count == 0


def test_relative_difference_above_tolerance_is_a_failure():
    # 1e-6 of the value is well above the 1e-9 relative tolerance and must
    # surface as a real diff.
    cmp = _comparator()
    control = _payload(({"a": "1"}, [(1.0, "1.0")]))
    test = _payload(({"a": "1"}, [(1.0, "1.000001")]))
    diff = _diff()

    cmp._compare_into(control, test, diff)

    assert diff.ok is False
    assert diff.failure_count == 1


def test_value_mismatch_records_failure_and_max_abs_diff():
    cmp = _comparator()
    control = _payload(({"a": "1"}, [(1.0, "5")]))
    test = _payload(({"a": "1"}, [(1.0, "5.1")]))
    diff = _diff()

    cmp._compare_into(control, test, diff)

    assert diff.ok is False
    assert diff.failure_count == 1
    assert math.isclose(diff.max_abs_diff, 0.1)
    assert len(diff.failures) == 1


def test_failure_cap_limits_stored_failures_but_not_count():
    cmp = _comparator(failure_cap=2)
    # 10 differing points; only 2 should be retained, but the count must
    # still report all 10.
    control = _payload(({"a": "1"}, [(float(i), str(i)) for i in range(10)]))
    test = _payload(({"a": "1"}, [(float(i), str(i + 1)) for i in range(10)]))
    diff = _diff()

    cmp._compare_into(control, test, diff)

    assert diff.failure_count == 10
    assert len(diff.failures) == 2


# ---------------------------------------------------------------------------
# _compare_into - NaN handling
# ---------------------------------------------------------------------------


def test_nan_on_both_sides_is_a_match():
    cmp = _comparator()
    payload = _payload(({"a": "1"}, [(1.0, "NaN")]))
    diff = _diff()

    cmp._compare_into(payload, payload, diff)

    assert diff.ok is True
    assert diff.failure_count == 0


def test_nan_against_finite_is_a_failure():
    cmp = _comparator()
    control = _payload(({"a": "1"}, [(1.0, "NaN")]))
    test = _payload(({"a": "1"}, [(1.0, "5")]))
    diff = _diff()

    cmp._compare_into(control, test, diff)

    assert diff.ok is False
    assert diff.failure_count == 1


# ---------------------------------------------------------------------------
# _compare_into - series-set mismatches
# ---------------------------------------------------------------------------


def test_series_only_in_control_is_recorded_as_issue():
    cmp = _comparator()
    control = _payload(
        ({"a": "1"}, []),
        ({"a": "2"}, []),
    )
    test = _payload(({"a": "1"}, []))
    diff = _diff()

    cmp._compare_into(control, test, diff)

    assert diff.ok is False
    assert any("only in control" in i.description for i in diff.series_issues)


def test_series_only_in_test_is_recorded_as_issue():
    cmp = _comparator()
    control = _payload(({"a": "1"}, []))
    test = _payload(
        ({"a": "1"}, []),
        ({"a": "2"}, []),
    )
    diff = _diff()

    cmp._compare_into(control, test, diff)

    assert diff.ok is False
    assert any("only in test" in i.description for i in diff.series_issues)


def test_timestamp_only_on_one_side_is_recorded_as_issue():
    cmp = _comparator()
    control = _payload(({"a": "1"}, [(1.0, "5"), (2.0, "5")]))
    test = _payload(({"a": "1"}, [(1.0, "5")]))
    diff = _diff()

    cmp._compare_into(control, test, diff)

    assert any("ts only in control" in i.description for i in diff.series_issues)
