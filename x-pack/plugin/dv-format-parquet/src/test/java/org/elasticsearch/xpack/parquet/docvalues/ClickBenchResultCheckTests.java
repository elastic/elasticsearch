/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.docvalues;

import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

/** The check of query results must accept every valid choice among ties and reject everything else. */
public class ClickBenchResultCheckTests extends ESTestCase {

    private static List<Object> row(Object... values) {
        return Arrays.asList(values);
    }

    /** A ranking by {@code c} of the groups {@code k}, top 3, with the expected rows a=9, b=5, c=5 and d=5 tied with the last row. */
    private static Map<String, Object> ranking() {
        Map<String, Object> expected = new HashMap<>();
        expected.put("cols", List.of("c", "k"));
        expected.put("outcols", List.of("c", "k"));
        expected.put("sort", List.of(List.of("c", "desc")));
        expected.put("limit", 3);
        expected.put("mode", "ties");
        expected.put("tolerance", List.of(0, 0));
        expected.put("group", null);
        expected.put("rows", List.of(row(9L, "a"), row(5L, "b"), row(5L, "c")));
        expected.put("ties", List.of(row(5L, "b"), row(5L, "c"), row(5L, "d")));
        expected.put("tiesComplete", true);
        return expected;
    }

    public void testTheRowsOfTheExpectedOrderAreValid() {
        assertNull(ClickBenchResultCheck.check(ranking(), List.of("c", "k"), List.of(row(9L, "a"), row(5L, "b"), row(5L, "c"))));
    }

    public void testAnyRowsThatTieWithTheLastRowAreValid() {
        assertNull(ClickBenchResultCheck.check(ranking(), List.of("c", "k"), List.of(row(9L, "a"), row(5L, "d"), row(5L, "b"))));
        // the columns are matched by name, whatever their order
        assertNull(ClickBenchResultCheck.check(ranking(), List.of("k", "c"), List.of(row("a", 9), row("c", 5), row("d", 5))));
    }

    public void testARowThatDoesNotTieWithTheLastRowIsNotValid() {
        assertThat(
            ClickBenchResultCheck.check(ranking(), List.of("c", "k"), List.of(row(9L, "a"), row(5L, "b"), row(5L, "z"))),
            containsString("not one of the expected rows")
        );
        // the first row has the right key but is not the group with that key, which is then missing
        assertThat(
            ClickBenchResultCheck.check(ranking(), List.of("c", "k"), List.of(row(9L, "z"), row(5L, "b"), row(5L, "c"))),
            containsString("expected but missing")
        );
    }

    public void testAWrongSortKeyOrAnotherNumberOfRowsIsNotValid() {
        assertThat(
            ClickBenchResultCheck.check(ranking(), List.of("c", "k"), List.of(row(8L, "a"), row(5L, "b"), row(5L, "c"))),
            containsString("sort key")
        );
        assertThat(ClickBenchResultCheck.check(ranking(), List.of("c", "k"), List.of(row(9L, "a"), row(5L, "b"))), containsString("rows"));
        assertThat(ClickBenchResultCheck.check(ranking(), List.of("c", "x"), List.of(row(9L, "a"))), containsString("columns"));
    }

    public void testARowThatIsDeterminedCannotBeReplacedByATiedRow() {
        // the key 9 has one group, so a=9 must be there, and a row with the key 5 cannot stand in for it
        Map<String, Object> expected = ranking();
        assertThat(
            ClickBenchResultCheck.check(expected, List.of("c", "k"), List.of(row(5L, "b"), row(5L, "c"), row(5L, "d"))),
            containsString("sort key")
        );
    }

    public void testTiesWithoutAnyCompleteListOnlyCheckTheKeys() {
        Map<String, Object> expected = ranking();
        expected.put("tiesComplete", false);
        expected.put("ties", List.of());
        assertNull(ClickBenchResultCheck.check(expected, List.of("c", "k"), List.of(row(9L, "a"), row(5L, "x"), row(5L, "y"))));
        assertThat(
            ClickBenchResultCheck.check(expected, List.of("c", "k"), List.of(row(9L, "q"), row(5L, "x"), row(5L, "y"))),
            containsString("missing")
        );
    }

    public void testASortKeyThatTheQueryDoesNotReturnChecksTheRowsAsASet() {
        Map<String, Object> expected = new HashMap<>();
        expected.put("cols", List.of("phrase", "time"));
        expected.put("outcols", List.of("phrase"));
        expected.put("sort", List.of(List.of("time", "asc")));
        expected.put("limit", 3);
        expected.put("mode", "ties");
        expected.put("tolerance", List.of(0, 0));
        expected.put("group", null);
        expected.put("rows", List.of(row("p", "t1"), row("q", "t2"), row("r", "t3")));
        expected.put("ties", List.of(row("r", "t3"), row("s", "t3")));
        expected.put("tiesComplete", true);
        // p and q come before the last time, and r or s may take the last place, in any order of the first two
        assertNull(ClickBenchResultCheck.check(expected, List.of("phrase"), List.of(row("q"), row("p"), row("s"))));
        assertThat(
            ClickBenchResultCheck.check(expected, List.of("phrase"), List.of(row("p"), row("p"), row("s"))),
            containsString("missing")
        );
    }

    public void testAnApproximateRankingAcceptsTopGroupsWithValuesInsideTheTolerance() {
        Map<String, Object> expected = new HashMap<>();
        expected.put("cols", List.of("u", "k"));
        expected.put("outcols", List.of("u", "k"));
        expected.put("sort", List.of(List.of("u", "desc")));
        expected.put("limit", 2);
        expected.put("mode", "approx_rank");
        expected.put("tolerance", List.of(0.05, 0));
        expected.put("group", List.of("k"));
        expected.put("rows", List.of(row(1000L, "a"), row(990L, "b"), row(900L, "c")));
        expected.put("ties", List.of());
        expected.put("tiesComplete", true);
        // the order of an approximate ranking may swap the near values, and the values may be off by a few percent
        assertNull(ClickBenchResultCheck.check(expected, List.of("u", "k"), List.of(row(1020L, "b"), row(980L, "a"))));
        assertThat(
            ClickBenchResultCheck.check(expected, List.of("u", "k"), List.of(row(1100L, "a"), row(980L, "b"))),
            containsString("exact value")
        );
        assertThat(
            ClickBenchResultCheck.check(expected, List.of("u", "k"), List.of(row(980L, "a"), row(990L, "b"))),
            containsString("larger value")
        );
        assertThat(
            ClickBenchResultCheck.check(expected, List.of("u", "k"), List.of(row(1000L, "a"), row(900L, "z"))),
            containsString("not one of the top groups")
        );
    }

    public void testCountOnlyChecksTheNumberOfRows() {
        Map<String, Object> expected = ranking();
        expected.put("mode", "count_only");
        assertNull(ClickBenchResultCheck.check(expected, List.of("c", "k"), List.of(row(1L, "x"), row(1L, "y"), row(1L, "z"))));
        assertThat(ClickBenchResultCheck.check(expected, List.of("c", "k"), List.of(row(1L, "x"))), containsString("rows"));
    }

    public void testNumbersUseTheTolerance() {
        assertTrue(ClickBenchResultCheck.same(1, 1L, 0));
        assertFalse(ClickBenchResultCheck.same(1, 2L, 0));
        assertTrue(ClickBenchResultCheck.same(283.7283855401811, 283.72838554018, 1e-6));
        assertFalse(ClickBenchResultCheck.same(283.7283855401811, 283.82, 1e-6));
        assertTrue("a tolerance above 1e-6 applies to whole numbers", ClickBenchResultCheck.same(1000L, 1020L, 0.05));
        assertTrue(ClickBenchResultCheck.same(null, null, 0));
        assertFalse(ClickBenchResultCheck.same(null, "x", 0));
    }
}
