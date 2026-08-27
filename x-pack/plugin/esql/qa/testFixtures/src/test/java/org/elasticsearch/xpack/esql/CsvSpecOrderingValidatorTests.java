/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

/**
 * Unit tests for {@link CsvSpecOrderingValidator}, which reports csv-spec tests whose expected row
 * order the query does not fully determine.
 * <p>
 * Cases are built by feeding text through the real {@link CsvSpecReader} parser, so the queries here
 * are exactly what a spec file would contain and are parsed by the real ES|QL grammar.
 * <p>
 * A query the grammar rejects makes the validator return {@code null}, which looks identical to "this
 * query is fine". Every test asserting {@code null} therefore pairs with one asserting a violation on
 * a near-identical query, so a typo that breaks parsing shows up as a failure rather than a silent
 * pass. {@link #testUnparseableQueryIsSkipped} pins that fallback deliberately.
 */
public class CsvSpecOrderingValidatorTests extends ESTestCase {

    private static final String TWO_DISTINCT_ROWS = """
        a:integer | b:keyword
        1         | one
        2         | two
        """;

    /**
     * Assembles a test case the way a spec file would: the query, then the expected result section.
     *
     * @return the reported violation, or {@code null} when the order is determined
     */
    private static String validate(String query, String expectedResults) {
        SpecReader.Parser parser = CsvSpecReader.specParser();
        parser.parse(query + ";");
        for (String line : expectedResults.split("\n")) {
            parser.parse(line);
        }
        Object result = parser.parse(";");
        assertThat(result, instanceOf(CsvTestCase.class));
        return CsvSpecOrderingValidator.validate((CsvTestCase) result);
    }

    private static void assertOrdered(String query, String expectedResults) {
        assertThat(validate(query, expectedResults), nullValue());
    }

    private static void assertUnordered(String query, String expectedResults) {
        assertThat(validate(query, expectedResults), containsString("nothing determines their order"));
    }

    private static void assertTied(String query, String expectedResults) {
        assertThat(validate(query, expectedResults), containsString("tie on sort key"));
    }

    // Cases the check deliberately does not judge

    public void testIgnoreOrderSkipsTheCheck() {
        assertOrdered("FROM employees | KEEP a, b", "ignoreOrder:true\n" + TWO_DISTINCT_ROWS);
        // Same query without the directive is reported, proving the directive is what silenced it.
        assertUnordered("FROM employees | KEEP a, b", TWO_DISTINCT_ROWS);
    }

    public void testRowSourceNeedsNoSort() {
        assertOrdered("ROW a = 1, b = \"one\"", TWO_DISTINCT_ROWS);
    }

    public void testSingleDataRowNeedsNoSort() {
        assertOrdered("FROM employees | KEEP a, b", """
            a:integer | b:keyword
            1         | one
            """);
    }

    public void testIdenticalDataRowsNeedNoSort() {
        assertOrdered("FROM employees | KEEP a, b", """
            a:integer | b:keyword
            1         | one
            1         | one
            """);
    }

    public void testUnparseableQueryIsSkipped() {
        assertOrdered("FROM employees | NOT_A_COMMAND ~~~", TWO_DISTINCT_ROWS);
    }

    // Missing order

    public void testMultipleRowsWithoutSortAreReported() {
        assertUnordered("FROM employees | KEEP a, b", TWO_DISTINCT_ROWS);
    }

    public void testSortFollowedByStatsIsReported() {
        assertUnordered("FROM employees | SORT a | STATS a = COUNT(*) BY b", TWO_DISTINCT_ROWS);
    }

    public void testSortFollowedByMvExpandIsReported() {
        assertUnordered("FROM employees | SORT a | MV_EXPAND b", TWO_DISTINCT_ROWS);
    }

    /**
     * ES|QL itself warns "SORT is followed by a LOOKUP JOIN which does not preserve order", so a SORT
     * before the join does not settle the output order.
     */
    public void testSortFollowedByLookupJoinIsReported() {
        assertUnordered("FROM employees | SORT a | LOOKUP JOIN languages_lookup ON language_code", TWO_DISTINCT_ROWS);
        assertOrdered("FROM employees | LOOKUP JOIN languages_lookup ON language_code | SORT a", TWO_DISTINCT_ROWS);
    }

    // Order established by a SORT, seen through commands that pass it along

    public void testSortIsEnough() {
        assertOrdered("FROM employees | SORT a", TWO_DISTINCT_ROWS);
    }

    public void testSortSurvivesOrderPreservingCommands() {
        assertOrdered("FROM employees | SORT a | LIMIT 5 | WHERE a > 0 | EVAL c = a + 1 | KEEP a, b | DROP c", TWO_DISTINCT_ROWS);
    }

    /**
     * MMR selects a subset of rows but emits them in input order, so a SORT before it still determines
     * the output order.
     */
    public void testMmrPassesThroughTheOrderBeforeIt() {
        String mmr = "| MMR [0.1, 0.2, 0.3] ON text_vector LIMIT 3 WITH { \"lambda\": 0.1 }";
        assertOrdered("FROM mmr_text_vector_keyword | SORT a | LIMIT 10 " + mmr, TWO_DISTINCT_ROWS);
        assertUnordered("FROM mmr_text_vector_keyword | LIMIT 10 " + mmr, TWO_DISTINCT_ROWS);
    }

    // Order established by the command itself

    /**
     * CHANGE_POINT's surrogate plan wraps its child in an OrderBy on the BY groupings then the ON key,
     * so it orders its own output regardless of how the preceding STATS emitted rows.
     */
    public void testChangePointOrdersItsOwnOutput() {
        assertOrdered(
            "FROM k8s | STATS count = COUNT() BY @timestamp = BUCKET(@timestamp, 1 MINUTE) "
                + "| CHANGE_POINT count ON @timestamp AS type, pvalue",
            TWO_DISTINCT_ROWS
        );
        // The same STATS without CHANGE_POINT has no order.
        assertUnordered("FROM k8s | STATS count = COUNT() BY @timestamp = BUCKET(@timestamp, 1 MINUTE)", TWO_DISTINCT_ROWS);
    }

    /**
     * Filling empty buckets requires sorting by group and bucket keys to interleave the generated rows,
     * so such a STATS orders its output where a plain STATS does not.
     */
    public void testStatsWithIncludeEmptyBucketsOrdersItsOwnOutput() {
        String bucket = "BUCKET(hire_date, 20, \"1985-01-01T00:00:00Z\", \"1986-01-01T00:00:00Z\", {\"include_empty_buckets\": true})";
        assertOrdered("FROM employees | STATS c = COUNT(*) BY hire_date = " + bucket, TWO_DISTINCT_ROWS);
    }

    /**
     * A second, plain STATS re-aggregates the ordered rows through a hash aggregation, which drops the
     * ordering the empty-bucket pass established.
     */
    public void testPlainStatsAfterIncludeEmptyBucketsIsReported() {
        String bucket = "BUCKET(hire_date, 20, \"1985-01-01T00:00:00Z\", \"1986-01-01T00:00:00Z\", {\"include_empty_buckets\": true})";
        assertUnordered(
            "FROM employees | STATS c = COUNT(*) BY hire_date = " + bucket + " | STATS c = COUNT(*) BY hire_date",
            TWO_DISTINCT_ROWS
        );
    }

    /**
     * The analyzer injects an implicit {@code @timestamp DESC} for a TS source, but only when no STATS
     * intervenes.
     */
    public void testTimeSeriesSourceOrdersItsOwnOutput() {
        assertOrdered("TS k8s | KEEP @timestamp, cluster, pod | LIMIT 3", TWO_DISTINCT_ROWS);
        assertUnordered("TS k8s | STATS c = COUNT(*) BY cluster", TWO_DISTINCT_ROWS);
    }

    // Sort keys that do not break every tie

    public void testAdjacentRowsTiedOnTheSortKeyAreReported() {
        assertTied("FROM employees | SORT a", """
            a:integer | b:keyword
            1         | one
            1         | uno
            """);
    }

    public void testFullyIdenticalTiedRowsAreNotReported() {
        // Rows tie on the key and match in every column, so swapping them changes nothing. A third,
        // distinct row keeps the all-rows-identical shortcut from being what passes this.
        assertOrdered("FROM employees | SORT a", """
            a:integer | b:keyword
            1         | one
            1         | one
            2         | two
            """);
    }

    public void testSecondSortKeyBreakingTheTieIsAccepted() {
        assertOrdered("FROM employees | SORT a, b", """
            a:integer | b:keyword
            1         | one
            1         | uno
            """);
    }

    public void testSortKeyMissingFromOutputIsSkipped() {
        assertOrdered("FROM employees | SORT missing_column | KEEP a, b", """
            a:integer | b:keyword
            1         | one
            1         | uno
            """);
    }

    public void testComplexSortExpressionIsSkipped() {
        assertOrdered("FROM employees | SORT ABS(a)", """
            a:integer | b:keyword
            1         | one
            1         | uno
            """);
    }

    public void testWildcardCellIsNotTreatedAsATie() {
        assertOrdered("FROM employees | SORT a", """
            a:keyword | b:keyword
            {any}     | one
            {any}     | uno
            """);
    }

    public void testSortDirectionAndNullsClauseDoNotHideTies() {
        assertTied("FROM employees | SORT a DESC NULLS LAST", """
            a:integer | b:keyword
            1         | one
            1         | uno
            """);
    }
}
