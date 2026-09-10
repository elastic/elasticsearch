/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.heap_attack;

import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.Build;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.test.ListMatcher;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;

/**
 * Tests that run ESQL queries with nested subqueries that use a ton of memory. We want to make sure they don't consume the entire heap
 * and crash Elasticsearch.
 */
@TimeoutSuite(millis = 40 * TimeUnits.MINUTE)
public class HeapAttackNestedSubqueryIT extends HeapAttackTestCase {

    // Reuse HeapAttackIT methods to prepare the indices
    private static final HeapAttackIT heapAttackIT = new HeapAttackIT();

    private static final int STRING_FIELDS_1K = 1000;

    private static final int STRING_FIELD_700 = 700;

    private static final int MAX_DOC = 100;

    private static final int NESTED_LEVELS = 3;

    private static final int BRANCHES_PER_LEVEL = 3;

    private static final int NESTED_LEAVES = nestedLeafCount(NESTED_LEVELS, BRANCHES_PER_LEVEL);

    @Before
    public void checkCapability() {
        assumeTrue("Run these tests in snapshot build", Build.current().isSnapshot());
    }

    /**
     * Fetches all 700 keyword fields from every leaf of the nested query. The 27-leaf union keeps far more keyword values than the
     * request breaker allows, so this must circuit-break while loading doc values or reserving memory for an intermediate page.
     */
    public void testManyRandomKeywordFieldsInNestedSubqueryIntermediateResults() throws IOException {
        heapAttackIT.initManyBigFieldsIndex(docs(), "keyword", true, STRING_FIELD_700);
        try {
            Map<String, Object> response = buildNestedSubqueries("manybigfields", serverlessExecuteBranchSequentially());
            fail("expected circuit_breaking_exception but query succeeded: " + response);
        } catch (ResponseException e) {
            verifyCircuitBreakingException(e);
        }
    }

    /**
     * Sorts each leaf by one keyword field before returning all 700 keyword fields. The Top-N materialization of those rows must
     * circuit-break.
     */
    public void testManyRandomKeywordFieldsInNestedSubqueryIntermediateResultsWithSortOneField() throws IOException {
        heapAttackIT.initManyBigFieldsIndex(docs(), "keyword", true, STRING_FIELD_700);
        try {
            Map<String, Object> response = buildNestedSubqueriesWithSort("manybigfields", "f000");
            fail("expected circuit_breaking_exception but query succeeded: " + response);
        } catch (ResponseException e) {
            verifyCircuitBreakingException(e);
        }
    }

    /**
     * Sorts each leaf by 11 keyword fields before returning all 700 keyword fields. Keeping the larger sort keys for all nested
     * branches must trip the request breaker while the Top-N operator adds input rows.
     */
    public void testManyRandomKeywordFieldsInNestedSubqueryIntermediateResultsWithSortManyFields() throws IOException {
        heapAttackIT.initManyBigFieldsIndex(docs(), "keyword", true, STRING_FIELD_700);
        StringBuilder sortKeys = new StringBuilder();
        sortKeys.append("f000");
        for (int f = 1; f < 11; f++) {
            sortKeys.append(", f").append(String.format(Locale.ROOT, "%03d", f));
        }
        try {
            Map<String, Object> response = buildNestedSubqueriesWithSort("manybigfields", sortKeys.toString());
            fail("expected circuit_breaking_exception but query succeeded: " + response);
        } catch (ResponseException e) {
            verifyCircuitBreakingException(e);
        }
    }

    /**
     * Sorts 1,000 numeric documents by 100 fields in every leaf. Numeric blocks are compact enough for this query to complete on the
     * stateful test cluster, while serverless may circuit break in the Top-N processing because of its different memory constraints.
     */
    public void testManyRandomNumericFieldsInNestedSubqueryIntermediateResultsWithSortManyFields() throws IOException {
        int docs = 1000;
        int fields = 1000;
        String type = randomFrom("integer", "long", "double");
        heapAttackIT.initManyBigFieldsIndex(docs, type, true, fields);
        StringBuilder sortKeys = new StringBuilder();
        sortKeys.append("f000");
        for (int f = 1; f < 100; f++) {
            sortKeys.append(", f").append(String.format(Locale.ROOT, "%03d", f));
        }
        ListMatcher columns = matchesList();
        for (int f = 0; f < fields; f++) {
            columns = columns.item(matchesMap().entry("name", "f" + String.format(Locale.ROOT, "%03d", f)).entry("type", type));
        }
        try {
            Map<?, ?> response = buildNestedSubqueriesWithSort("manybigfields", sortKeys.toString());
            assertMap(response, matchesMap().entry("columns", columns));
        } catch (ResponseException e) {
            verifyCircuitBreakingException(e);
        }
    }

    /**
     * Aggregates one keyword field without grouping in every leaf. Each leaf reduces its input to one row before the union, so the query
     * is expected to complete without circuit breaking and return one result for each leaf.
     */
    public void testManyRandomKeywordFieldsInNestedSubqueryIntermediateResultsWithAggNoGrouping() throws IOException {
        int docs = docs();
        heapAttackIT.initManyBigFieldsIndex(docs, "keyword", true, STRING_FIELDS_1K);
        ListMatcher columns = matchesList().item(matchesMap().entry("name", "sum").entry("type", "long"));
        Map<?, ?> response = buildNestedSubqueriesWithAgg("manybigfields", "sum = SUM(LENGTH(f999))", null);
        ListMatcher values = matchesList();
        for (int i = 0; i < NESTED_LEAVES; i++) {
            values = values.item(matchesList().item(1024 * docs));
        }
        assertMap(response, matchesMap().entry("columns", columns).entry("values", values));
    }

    /**
     * Aggregates one keyword field and groups by one keyword field in every leaf. Reducing each branch to at most one row per group keeps
     * the union's intermediate results small enough to complete without circuit breaking.
     */
    public void testManyRandomKeywordFieldsInNestedSubqueryIntermediateResultsWithAggWithGBYOneField() throws IOException {
        int docs = docs();
        heapAttackIT.initManyBigFieldsIndex(docs, "keyword", true, STRING_FIELDS_1K);
        var columns = List.of(Map.of("name", "sum", "type", "long"), Map.of("name", "f000", "type", "keyword"));
        Map<?, ?> response = buildNestedSubqueriesWithAgg("manybigfields", "sum = SUM(LENGTH(f999))", "f000");
        var values = response.get("values");
        assertEquals(columns, response.get("columns"));
        assertTrue(values instanceof List<?> l && l.size() <= docs * NESTED_LEAVES);
    }

    /**
     * Aggregates each leaf using 100 keyword grouping fields. The stateful test cluster can complete this query, but environments with a
     * lower request-breaker allowance may circuit break while loading the grouping values or retaining the per-branch aggregation state.
     */
    public void testManyRandomKeywordFieldsInNestedSubqueryIntermediateResultsWithAggGBYManyFields() throws IOException {
        int docs = docs();
        heapAttackIT.initManyBigFieldsIndex(docs, "keyword", true, STRING_FIELD_700);
        StringBuilder grouping = new StringBuilder();
        grouping.append("f000");
        int groupBySize = 100;
        for (int f = 1; f < groupBySize; f++) {
            grouping.append(", f").append(String.format(Locale.ROOT, "%03d", f));
        }
        try {
            Map<?, ?> response = buildNestedSubqueriesWithAgg("manybigfields", "c = COUNT_DISTINCT(f499)", grouping.toString());
            assertTrue(response.get("columns") instanceof List<?> l && l.size() == (groupBySize + 1));
        } catch (ResponseException e) {
            verifyCircuitBreakingException(e);
        }
    }

    private Map<String, Object> buildNestedSubqueries(String indexName, Integer branchParallelDegree) throws IOException {
        StringBuilder query = startQuery();
        query.append(nestedFrom(indexName, "")).append(endQuery(branchParallelDegree));
        return responseAsMap(query(query.toString(), "columns"));
    }

    private Map<String, Object> buildNestedSubqueriesWithSort(String indexName, String sortKeys) throws IOException {
        StringBuilder query = startQuery();
        query.append(nestedFrom(indexName, " | SORT " + sortKeys)).append(endQuery(serverlessExecuteBranchSequentially()));
        return responseAsMap(query(query.toString(), "columns"));
    }

    private Map<String, Object> buildNestedSubqueriesWithAgg(String indexName, String aggregation, String grouping) throws IOException {
        StringBuilder pipeline = new StringBuilder(" | STATS ").append(aggregation);
        if (grouping != null && grouping.isEmpty() == false) {
            pipeline.append(" BY ").append(grouping);
        }
        StringBuilder query = startQuery();
        query.append(nestedFrom(indexName, pipeline.toString())).append(endQuery());
        return responseAsMap(query(query.toString(), "columns,values"));
    }

    /**
     * Builds a regular UNION ALL tree. The returned query text has the following recursive shape, where each child is repeated
     * {@link #BRANCHES_PER_LEVEL} times at every level:
     * <pre>{@code
     * FROM
     *   (FROM
     *     ...
     *     (FROM indexName processingCommands),
     *     ...
     *   ),
     *   ...
     * }</pre>
     */
    private static String nestedFrom(String indexName, String processingCommands) {
        String child = "(FROM " + indexName + processingCommands + " )";
        for (int level = NESTED_LEVELS; level > 1; level--) {
            child = "(FROM " + repeat(BRANCHES_PER_LEVEL, child) + ")";
        }
        return "FROM " + repeat(BRANCHES_PER_LEVEL, child);
    }

    private static String repeat(int branches, String child) {
        StringBuilder sb = new StringBuilder(child);
        for (int i = 1; i < branches; i++) {
            sb.append(", ").append(child);
        }
        return sb.toString();
    }

    private static int nestedLeafCount(int maxNestedLevel, int maxBranchPerLevel) {
        int leaves = 1;
        for (int i = 0; i < maxNestedLevel; i++) {
            leaves *= maxBranchPerLevel;
        }
        return leaves;
    }

    private static String endQuery() {
        return endQuery(null);
    }

    private static String endQuery(Integer branchParallelDegree) {
        if (branchParallelDegree != null) {
            return " \", \"pragma\": {\"branch_parallel_degree\": " + branchParallelDegree + "}}";
        }
        return " \"}";
    }

    private static Integer serverlessExecuteBranchSequentially() throws IOException {
        return isServerless() ? 1 : null;
    }

    private static int docs() {
        return MAX_DOC;
    }

    private static void verifyCircuitBreakingException(ResponseException re) throws IOException {
        Map<?, ?> map = responseAsMap(re.getResponse());
        assertMap(
            map,
            matchesMap().entry("status", 429).entry("error", matchesMap().extraOk().entry("type", "circuit_breaking_exception"))
        );
    }
}
