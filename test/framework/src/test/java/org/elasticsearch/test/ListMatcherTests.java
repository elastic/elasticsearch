/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.elasticsearch.test.MapMatcherTests.SUBMATCHER;
import static org.elasticsearch.test.MapMatcherTests.SUBMATCHER_ERR;
import static org.elasticsearch.test.MapMatcherTests.assertDescribeTo;
import static org.elasticsearch.test.MapMatcherTests.assertMismatch;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class ListMatcherTests extends ESTestCase {
    public void testEmptyMatchesEmpty() {
        assertThat(List.of(), matchesList());
    }

    public void testEmptyMismatch() {
        assertMismatch(List.of(1), matchesList(), equalTo("""
            an empty list
            0: <unexpected> but was <1>"""));
    }

    public void testExpectedEmptyMismatch() {
        assertMismatch(List.of("foo", "bar"), matchesList(), equalTo("""
            an empty list
            0: <unexpected> but was "foo"
            1: <unexpected> but was "bar"
            """.strip()));
    }

    public void testMissing() {
        assertMismatch(List.of(), matchesList().item("foo"), equalTo("""
            a list containing
            0: expected "foo" but was <missing>"""));
    }

    public void testWrongSimpleValue() {
        assertMismatch(List.of("foo"), matchesList().item("bar"), equalTo("""
            a list containing
            0: expected "bar" but was "foo"
            """.strip()));
    }

    public void testExtra() {
        StringBuilder mismatch = new StringBuilder();
        mismatch.append("a list containing\n");
        mismatch.append("0: <1>\n");
        mismatch.append("1: <unexpected> but was <2>");
        assertMismatch(List.of(1, 2), matchesList().item(1), equalTo(mismatch.toString()));
    }

    public void testManyExtra() {
        assertMismatch(List.of(1, 2, 3), matchesList().item(1), equalTo("""
            a list containing
            0: <1>
            1: <unexpected> but was <2>
            2: <unexpected> but was <3>"""));
    }

    public void testManyWrongSimpleValue() {
        assertMismatch(List.of(5, 6, 7), matchesList().item(1).item(6).item(10), equalTo("""
            a list containing
            0: expected <1> but was <5>
            1: <6>
            2: expected <10> but was <7>"""));
    }

    public void testNullValue() {
        List<Object> list = new ArrayList<>();
        list.add("foo");
        list.add(null);
        assertMap(list, expectNull());
    }

    public void testExpectedNull() {
        assertMismatch(List.of("foo", "bar"), expectNull(), equalTo("""
            a list containing
            0: "foo"
            1: expected null but was "bar"
            """.trim()));
    }

    private ListMatcher expectNull() {
        return matchesList().item("foo").item(null);
    }

    public void testExpectedButWasNull() {
        List<Object> list = new ArrayList<>();
        list.add("foo");
        list.add(null);
        assertMismatch(list, matchesList().item("foo").item("bar"), equalTo("""
            a list containing
            0: "foo"
            1: expected "bar" but was null"""));
    }

    public void testSubMap() {
        assertMismatch(List.of(Map.of("bar", 2), 2), matchesList().item(Map.of("bar", 1)).item(2), equalTo("""
            a list containing
            0: a map containing
            bar: expected <1> but was <2>
            1: <2>"""));
    }

    public void testSubMapMismatchEmpty() {
        assertMismatch(List.of(1), matchesList().item(1).item(Map.of("bar", 1)), equalTo("""
            a list containing
            0: <1>
            1: expected a map but was <missing>"""));
    }

    public void testSubMapMatcher() {
        assertMismatch(List.of(Map.of("bar", 2), 2), matchesList().item(matchesMap().entry("bar", 1)).item(2), equalTo("""
            a list containing
            0: a map containing
            bar: expected <1> but was <2>
            1: <2>"""));
    }

    public void testSubEmptyMap() {
        StringBuilder mismatch = new StringBuilder();
        mismatch.append("a list containing\n");
        mismatch.append("0: an empty map\n");
        mismatch.append("bar: <unexpected> but was <2>\n");
        mismatch.append("1: <2>");
        assertMismatch(List.of(Map.of("bar", 2), 2), matchesList().item(Map.of()).item(2), equalTo(mismatch.toString()));
    }

    void testSubList() {
        assertMismatch(List.of(List.of(2), 2), matchesList().item(List.of(1)).item(2), equalTo("""
            a list containing
            0: a list containing
              0: expected <1> but was <2>
            1: <2>"""));
    }

    public void testSubListMismatchEmpty() {
        assertMismatch(List.of(1), matchesList().item(1).item(List.of("bar", 1)), equalTo("""
            a list containing
            0: <1>
            1: expected a list but was <missing>"""));
    }

    public void testSubListMatcher() {
        assertMismatch(List.of(List.of(2), 2), matchesList().item(matchesList().item(1)).item(2), equalTo("""
            a list containing
            0: a list containing
              0: expected <1> but was <2>
            1: <2>"""));
    }

    public void testSubEmptyList() {
        StringBuilder mismatch = new StringBuilder();
        mismatch.append("a list containing\n");
        mismatch.append("0: an empty list\n");
        mismatch.append("  0: <unexpected> but was <2>\n");
        mismatch.append("1: <2>");
        assertMismatch(List.of(List.of(2), 2), matchesList().item(List.of()).item(2), equalTo(mismatch.toString()));
    }

    public void testSubMatcher() {
        assertMismatch(List.of(2.0, 2), matchesList().item(SUBMATCHER).item(2), equalTo("""
            a list containing
            0: %ERR
            1: <2>""".replace("%ERR", SUBMATCHER_ERR)));
    }

    public void testSubMatcherAsValue() {
        Object item0 = SUBMATCHER;
        assertMismatch(List.of(2.0, 2), matchesList().item(item0).item(2), equalTo("""
            a list containing
            0: %ERR
            1: <2>""".replace("%ERR", SUBMATCHER_ERR)));
    }

    public void testProvideList() {
        assertMismatch(
            List.of(List.of(1), Map.of("bar", 2), 2.0),
            matchesList(List.of(List.of(1), Map.of("bar", 1), closeTo(1.0, 0.5))),
            equalTo("""
                a list containing
                0: a list containing
                  0: <1>
                1: a map containing
                bar: expected <1> but was <2>
                2: %ERR""".replace("%ERR", SUBMATCHER_ERR))
        );
    }

    public void testProvideMapContainingNullMatch() {
        List<Object> list = new ArrayList<>();
        list.add(1);
        list.add(null);
        assertMap(list, provideListContainingNull());
    }

    public void testProvideMapContainingNullMismatch() {
        assertMismatch(List.of(1, "c"), provideListContainingNull(), equalTo("""
            a list containing
            0: <1>
            1: expected null but was "c"
            """.trim()));
    }

    private ListMatcher provideListContainingNull() {
        List<Object> spec = new ArrayList<>();
        spec.add(1);
        spec.add(null);
        return matchesList(spec);
    }

    public void testImmutable() {
        ListMatcher matcher = matchesList();
        assertMap(List.of("a"), matcher.item("a"));
        assertMap(List.of(), matcher);
    }

    public void testEmptyDescribeTo() {
        assertDescribeTo(matchesList(), equalTo("an empty list"));
    }

    public void testSimpleDescribeTo() {
        assertDescribeTo(matchesList().item(1).item(3), equalTo("""
            a list containing
            0: <1>
            1: <3>"""));
    }

    public void testSubListDescribeTo() {
        assertDescribeTo(matchesList().item(1).item(matchesList().item(0)), equalTo("""
            a list containing
            0: <1>
            1: a list containing
              0: <0>"""));
    }

    public void testSubMapDescribeTo() {
        assertDescribeTo(matchesList().item(1).item(matchesMap().entry("foo", 0)), equalTo("""
            a list containing
            0: <1>
            1: a map containing
            foo: <0>"""));
    }
}
