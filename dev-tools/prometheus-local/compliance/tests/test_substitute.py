#  Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
#  or more contributor license agreements. Licensed under the "Elastic License
#  2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
#  Public License v 1"; you may not use this file except in compliance with, at
#  your election, the "Elastic License 2.0", the "GNU Affero General Public
#  License v3.0 only", or the "Server Side Public License, v 1".

"""Tests for `util.substitute` - the parser that walks a PromQL string and
swaps `$var` / `${var}` references against a kv map. The behavioural
contract is:

  * known vars are substituted everywhere
  * unknown vars OUTSIDE string literals are left intact (still parseable
    PromQL syntactically)
  * unknown vars INSIDE string literals are replaced with the placeholder
    `__unknown__` and the returned ok flag becomes False so callers can
    skip the query
  * quote tracking respects both `\"` and `'`
"""

from util import substitute


def test_no_vars_passes_through_unchanged():
    assert substitute("foo bar", {}) == (True, "foo bar")


def test_known_var_outside_string_substitutes():
    assert substitute("a $x b", {"$x": "X"}) == (True, "a X b")


def test_known_var_in_double_quoted_string_substitutes():
    assert substitute('a "$x" b', {"$x": "X"}) == (True, 'a "X" b')


def test_known_var_in_single_quoted_string_substitutes():
    assert substitute("a '$x' b", {"$x": "X"}) == (True, "a 'X' b")


def test_unknown_var_outside_string_marks_not_ok_and_keeps_raw_text():
    # No valid PromQL placeholder exists for a bare `$var` outside a string
    # literal, so we leave the raw text and let the loader drop the query -
    # both ES and Prometheus reject the resulting expression at parse time.
    ok, out = substitute("a $x b", {})
    assert ok is False
    assert out == "a $x b"


def test_unknown_var_in_string_uses_placeholder_and_marks_not_ok():
    ok, out = substitute('a "$x" b', {})
    assert ok is False
    assert out == 'a "__unknown__" b'


def test_braced_var_form_substitutes():
    assert substitute("a ${x} b", {"$x": "X"}) == (True, "a X b")


def test_bare_dollar_left_intact():
    # `$` not followed by a name char emits a literal `$`.
    assert substitute("price: $ today", {}) == (True, "price: $ today")


def test_empty_braces_left_intact():
    # `${}` has no name to capture; emits the literal `${}`.
    assert substitute("a ${} b", {}) == (True, "a ${} b")


def test_separate_quote_styles_dont_confuse_each_other():
    # The opening `'` must be paired with the closing `'`, not the embedded `"`.
    assert substitute('"x" \'$v\' "y"', {"$v": "V"}) == (True, '"x" \'V\' "y"')


def test_multiple_vars_in_one_query():
    assert substitute('a "$x" $y c', {"$x": "X", "$y": "Y"}) == (True, 'a "X" Y c')


def test_mixed_known_and_unknown_in_string_marks_not_ok():
    ok, out = substitute('"$known $unknown"', {"$known": "K"})
    assert ok is False
    assert out == '"K __unknown__"'


def test_var_name_includes_underscores_and_digits():
    assert substitute("$rate_interval_2", {"$rate_interval_2": "5m"}) == (True, "5m")


def test_adjacent_vars_substitute_independently():
    assert substitute("$a$b", {"$a": "A", "$b": "B"}) == (True, "AB")
