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
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;

/**
 * Tests that run ESQL queries with subqueries that use a ton of memory. We want to make
 * sure they don't consume the entire heap and crash Elasticsearch.
 */
@TimeoutSuite(millis = 40 * TimeUnits.MINUTE)
public class HeapAttackSubqueryIT extends HeapAttackTestCase {

    // Reuse HeapAttackIT methods to prepare the indices
    private static final HeapAttackIT heapAttackIT = new HeapAttackIT();

    private static final int MIN_SUBQUERIES = 2;

    // the upper limit is defined in {@code Fork.MAX_BRANCHES}
    private static final int MAX_SUBQUERIES = 8;

    private static final int MAX_SUBQUERIES_SERVERLESS = 5;

    private static final int STRING_FIELDS_1K = 1000;

    private static final int STRING_FIELD_700 = 700;

    private static final int MAX_DOC = 100;

    @Before
    public void checkCapability() {
        assumeTrue("Run these tests in snapshot build", Build.current().isSnapshot());
    }

    public void testManyKeywordFieldsWith10UniqueValuesInSubqueryIntermediateResults() throws IOException {
        heapAttackIT.initManyBigFieldsIndex(MAX_DOC, "keyword", false, STRING_FIELDS_1K);
        for (int subquery : List.of(MIN_SUBQUERIES, MAX_SUBQUERIES)) {
            ListMatcher columns = matchesList();
            int fieldsToRead = subquery < MAX_SUBQUERIES ? STRING_FIELDS_1K : STRING_FIELD_700; // with 1000 fields we circuit break
            StringBuilder query = new StringBuilder("manybigfields | KEEP ");
            for (int f = 0; f < fieldsToRead; f++) {
                String fieldName = "f" + String.format(Locale.ROOT, "%03d", f);
                columns = columns.item(matchesMap().entry("name", fieldName).entry("type", "keyword"));
                if (f != 0) {
                    query.append(", ");
                }
                query.append('f').append(String.format(Locale.ROOT, "%03d", f));
            }
            Map<?, ?> response = buildSubqueries(subquery, query.toString());
            assertMap(response, matchesMap().entry("columns", columns));
        }
    }

    public void testManyRandomKeywordFieldsInSubqueryIntermediateResults() throws IOException {
        int docs = docs();
        heapAttackIT.initManyBigFieldsIndex(docs, "keyword", true, STRING_FIELD_700);
        ListMatcher columns = matchesList();
        for (int f = 0; f < STRING_FIELD_700; f++) {
            columns = columns.item(matchesMap().entry("name", "f" + String.format(Locale.ROOT, "%03d", f)).entry("type", "keyword"));
        }
        try {
            Map<?, ?> response = buildSubqueries(subqueries(false), "manybigfields", serverlessExecuteBranchSequentially());
            assertMap(response, matchesMap().entry("columns", columns));
        } catch (ResponseException e) {
            verifyCircuitBreakingException(e);
        }
    }

    public void testManyRandomKeywordFieldsInSubqueryIntermediateResultsWithSortOneField() throws IOException {
        int docs = docs();
        heapAttackIT.initManyBigFieldsIndex(docs, "keyword", true, STRING_FIELD_700);
        ListMatcher columns = matchesList();
        for (int f = 0; f < STRING_FIELD_700; f++) {
            columns = columns.item(matchesMap().entry("name", "f" + String.format(Locale.ROOT, "%03d", f)).entry("type", "keyword"));
        }
        try {
            Map<?, ?> response = buildSubqueriesWithSort(subqueries(true), "manybigfields", "f000");
            assertMap(response, matchesMap().entry("columns", columns));
        } catch (ResponseException e) {
            verifyCircuitBreakingException(e);
        }
    }

    public void testManyRandomKeywordFieldsInSubqueryIntermediateResultsWithSortManyFields() throws IOException {
        int docs = docs();
        heapAttackIT.initManyBigFieldsIndex(docs, "keyword", true, STRING_FIELD_700);
        StringBuilder sortKeys = new StringBuilder();
        sortKeys.append("f000");
        for (int f = 1; f < 11; f++) {
            sortKeys.append(", f").append(String.format(Locale.ROOT, "%03d", f));
        }
        ListMatcher columns = matchesList();
        for (int f = 0; f < STRING_FIELD_700; f++) {
            columns = columns.item(matchesMap().entry("name", "f" + String.format(Locale.ROOT, "%03d", f)).entry("type", "keyword"));
        }
        try {
            Map<?, ?> response = buildSubqueriesWithSort(subqueries(true), "manybigfields", sortKeys.toString());
            assertMap(response, matchesMap().entry("columns", columns));
        } catch (ResponseException e) {
            verifyCircuitBreakingException(e);
        }
    }

    public void testManyRandomNumericFieldsInSubqueryIntermediateResultsWithSortManyFields() throws IOException {
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
        // results are returned from non-serverless environment, but CBE is expected in serverless
        try {
            Map<?, ?> response = buildSubqueriesWithSort(MAX_SUBQUERIES, "manybigfields", sortKeys.toString());
            assertMap(response, matchesMap().entry("columns", columns));
        } catch (ResponseException e) {
            verifyCircuitBreakingException(e);
        }
    }

    public void testManyRandomTextFieldsInSubqueryIntermediateResults() throws IOException {
        int docs = docs();
        heapAttackIT.initManyBigFieldsIndex(docs, "text", true, STRING_FIELD_700);
        ListMatcher columns = matchesList();
        for (int f = 0; f < STRING_FIELD_700; f++) {
            columns = columns.item(matchesMap().entry("name", "f" + String.format(Locale.ROOT, "%03d", f)).entry("type", "text"));
        }
        try {
            Map<?, ?> response = buildSubqueries(subqueries(false), "manybigfields", serverlessExecuteBranchSequentially());
            assertMap(response, matchesMap().entry("columns", columns));
        } catch (ResponseException e) {
            verifyCircuitBreakingException(e);
        }
    }

    public void testManyRandomTextFieldsInSubqueryIntermediateResultsWithSortOneField() throws IOException {
        int docs = docs();
        heapAttackIT.initManyBigFieldsIndex(docs, "text", true, STRING_FIELD_700);
        ListMatcher columns = matchesList();
        for (int f = 0; f < STRING_FIELD_700; f++) {
            columns = columns.item(matchesMap().entry("name", "f" + String.format(Locale.ROOT, "%03d", f)).entry("type", "text"));
        }
        try {
            Map<?, ?> response = buildSubqueriesWithSort(subqueries(true), "manybigfields", " substring(f000, 5) ");
            assertMap(response, matchesMap().entry("columns", columns));
        } catch (ResponseException e) {
            verifyCircuitBreakingException(e);
        }
    }

    public void testManyRandomTextFieldsInSubqueryIntermediateResultsWithSortManyFields() throws IOException {
        int docs = docs();
        heapAttackIT.initManyBigFieldsIndex(docs, "text", true, STRING_FIELD_700);
        StringBuilder sortKeys = new StringBuilder();
        sortKeys.append(" substring(f000, 5) ");
        for (int f = 1; f < 5; f++) {
            sortKeys.append(", substring(f").append(String.format(Locale.ROOT, "%03d", f)).append(", 5) ");
        }
        ListMatcher columns = matchesList();
        for (int f = 0; f < STRING_FIELD_700; f++) {
            columns = columns.item(matchesMap().entry("name", "f" + String.format(Locale.ROOT, "%03d", f)).entry("type", "text"));
        }
        try {
            Map<?, ?> response = buildSubqueriesWithSort(subqueries(true), "manybigfields", sortKeys.toString());
            assertMap(response, matchesMap().entry("columns", columns));
        } catch (ResponseException e) {
            verifyCircuitBreakingException(e);
        }
    }

    public void testManyKeywordFieldsWith10UniqueValuesInSubqueryIntermediateResultsWithAggNoGrouping() throws IOException {
        int docs = docs();
        heapAttackIT.initManyBigFieldsIndex(docs, "keyword", false, STRING_FIELDS_1K);
        ListMatcher columns = matchesList().item(matchesMap().entry("name", "sum").entry("type", "long"));
        Map<?, ?> response = buildSubqueriesWithAgg(MAX_SUBQUERIES, "manybigfields", "sum = SUM(LENGTH(f999))", null);
        ListMatcher values = matchesList();
        for (int i = 0; i < MAX_SUBQUERIES; i++) {
            values = values.item(matchesList().item(1024 * docs));
        }
        assertMap(response, matchesMap().entry("columns", columns).entry("values", values));
    }

    public void testManyRandomKeywordFieldsInSubqueryIntermediateResultsWithAggNoGrouping() throws IOException {
        int docs = docs();
        heapAttackIT.initManyBigFieldsIndex(docs, "keyword", true, STRING_FIELDS_1K);
        ListMatcher columns = matchesList().item(matchesMap().entry("name", "sum").entry("type", "long"));
        Map<?, ?> response = buildSubqueriesWithAgg(MAX_SUBQUERIES, "manybigfields", "sum = SUM(LENGTH(f999))", null);
        ListMatcher values = matchesList();
        for (int i = 0; i < MAX_SUBQUERIES; i++) {
            values = values.item(matchesList().item(1024 * docs));
        }
        assertMap(response, matchesMap().entry("columns", columns).entry("values", values));
    }

    public void testManyRandomKeywordFieldsInSubqueryIntermediateResultsWithAggWithGBYOneField() throws IOException {
        int docs = docs();
        heapAttackIT.initManyBigFieldsIndex(docs, "keyword", true, STRING_FIELDS_1K);
        var columns = List.of(Map.of("name", "sum", "type", "long"), Map.of("name", "f000", "type", "keyword"));
        Map<?, ?> response = buildSubqueriesWithAgg(MAX_SUBQUERIES, "manybigfields", "sum = SUM(LENGTH(f999))", "f000");
        var values = response.get("values");
        assertEquals(columns, response.get("columns"));
        assertTrue(values instanceof List<?> l && l.size() <= docs * MAX_SUBQUERIES);
    }

    public void testManyRandomKeywordFieldsInSubqueryIntermediateResultsWithAggGBYManyFields() throws IOException {
        int docs = docs();
        heapAttackIT.initManyBigFieldsIndex(docs, "keyword", true, STRING_FIELD_700);
        StringBuilder grouping = new StringBuilder();
        grouping.append("f000");
        int groupBySize = 100;
        for (int f = 1; f < groupBySize; f++) {
            grouping.append(", f").append(String.format(Locale.ROOT, "%03d", f));
        }
        try {
            Map<?, ?> response = buildSubqueriesWithAgg(
                subqueries(false),
                "manybigfields",
                "c = COUNT_DISTINCT(f499)",
                grouping.toString()
            );
            assertTrue(response.get("columns") instanceof List<?> l && l.size() == (groupBySize + 1));
        } catch (ResponseException e) {
            verifyCircuitBreakingException(e);
        }
    }

    public void testGiantTextFieldInSubqueryIntermediateResults() throws IOException {
        int docs = 20;
        heapAttackIT.initGiantTextField(docs, false, 5);
        assertCircuitBreaks(attempt -> buildSubqueries(subqueries(false), "bigtext", serverlessExecuteBranchSequentially()));
    }

    public void testGiantTextFieldInSubqueryIntermediateResultsWithSort() throws IOException {
        int docs = 10;
        heapAttackIT.initGiantTextField(docs, false, 5);
        try {
            Map<?, ?> response = buildSubqueriesWithSort(subqueries(true), "bigtext", " substring(f, 5) ");
            assertTrue(response.get("columns") instanceof List<?> l && l.size() == 1);
        } catch (ResponseException e) {
            verifyCircuitBreakingException(e);
        }
    }

    public void testGiantTextFieldInSubqueryIntermediateResultsWithAggNoGrouping() throws IOException {
        int docs = 100;
        heapAttackIT.initGiantTextField(docs, false, 5);
        ListMatcher columns = matchesList().item(matchesMap().entry("name", "sum").entry("type", "long"));
        Map<?, ?> response = buildSubqueriesWithAgg(MAX_SUBQUERIES, "bigtext", "sum = SUM(LENGTH(f))", null);
        ListMatcher values = matchesList();
        for (int i = 0; i < MAX_SUBQUERIES; i++) {
            values = values.item(matchesList().item(1024 * 1024 * 5 * docs));
        }
        assertMap(response, matchesMap().entry("columns", columns).entry("values", values));
    }

    public void testLoadDocSequenceReturnsCorrectResultsKeyword() throws IOException {
        int docs = 20;
        heapAttackIT.initManyBigFieldsIndex(docs, "keyword", false, STRING_FIELDS_1K);
        verifyLoadDocSequenceResults(docs, "keyword");
    }

    public void testLoadDocSequenceReturnsCorrectResultsText() throws IOException {
        int docs = 20;
        heapAttackIT.initManyBigFieldsIndex(docs, "text", false, STRING_FIELDS_1K);
        verifyLoadDocSequenceResults(docs, "text");
    }

    private void verifyLoadDocSequenceResults(int docs, String dataType) throws IOException {
        List<Map<String, String>> columns = new ArrayList<>();
        for (int f = 0; f < STRING_FIELDS_1K; f++) {
            Map<String, String> column = Map.of("name", "f" + String.format(Locale.ROOT, "%03d", f), "type", dataType);
            columns.add(column);
        }

        // serverless triggers CBE occasionally.
        try {
            Map<?, ?> response = buildSubqueriesWithSortInMainQuery(MAX_SUBQUERIES, "manybigfields", "f000");
            assertEquals(columns, response.get("columns"));

            List<?> values = (List<?>) response.get("values");
            assertEquals(docs * MAX_SUBQUERIES, values.size());

            for (Object rowObj : values) {
                List<?> row = (List<?>) rowObj;
                assertEquals(1000, row.size());
                for (int f = 0; f < 1000; f++) {
                    assertEquals(Integer.toString(f % 10).repeat(1024), row.get(f));
                }
            }
        } catch (ResponseException e) {
            verifyCircuitBreakingException(e);
        }
    }

    private Map<String, Object> buildSubqueries(int subqueries, String indexName) throws IOException {
        return buildSubqueries(subqueries, indexName, null);
    }

    private Map<String, Object> buildSubqueries(int subqueries, String indexName, Integer branchParallelDegree) throws IOException {
        StringBuilder query = startQuery();
        String subquery = "(FROM " + indexName + " )";
        query.append("FROM ").append(subquery);
        for (int i = 1; i < subqueries; i++) {
            query.append(", ").append(subquery);
        }
        query.append(endQuery(branchParallelDegree));
        return responseAsMap(query(query.toString(), "columns"));
    }

    private Map<String, Object> buildSubqueriesWithAgg(int subqueries, String indexName, String aggregation, String grouping)
        throws IOException {
        StringBuilder query = startQuery();
        StringBuilder subquery = new StringBuilder();
        subquery.append("(FROM ").append(indexName).append(" | STATS ").append(aggregation);
        if (grouping != null && grouping.isEmpty() == false) {
            subquery.append(" BY ").append(grouping);
        }
        subquery.append(" )");
        query.append("FROM ").append(subquery);
        for (int i = 1; i < subqueries; i++) {
            query.append(", ").append(subquery);
        }
        query.append(endQuery());
        return responseAsMap(query(query.toString(), "columns,values"));
    }

    private Map<String, Object> buildSubqueriesWithSort(int subqueries, String indexName, String sortKeys) throws IOException {
        StringBuilder query = startQuery();
        StringBuilder subquery = new StringBuilder();
        subquery.append("(FROM ").append(indexName).append(" | SORT ").append(sortKeys).append(" )");
        query.append("FROM ").append(subquery);
        for (int i = 1; i < subqueries; i++) {
            query.append(", ").append(subquery);
        }
        query.append(endQuery());
        return responseAsMap(query(query.toString(), "columns"));
    }

    private Map<String, Object> buildSubqueriesWithSortInMainQuery(int subqueries, String indexName, String sortKeys) throws IOException {
        StringBuilder query = startQuery();
        String subquery = "(FROM " + indexName + " )";
        query.append("FROM ").append(subquery);
        for (int i = 1; i < subqueries; i++) {
            query.append(", ").append(subquery);
        }
        query.append(" | SORT ").append(sortKeys).append(endQuery());
        return responseAsMap(query(query.toString(), "columns, values"));
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

    /*
     * Serverless has 6 shards, non-serverless has 1 shard.
     * The number of exchange operators increases when the number of subqueries increase.
     * Sort is expensive as these tests preserve all fields comparing to stats.
     * So, limiting the number of subqueries to reduce the gc lagging and intermittent OOM.
     */
    private static int subqueries(boolean hasSort) throws IOException {
        if (isServerless()) {
            return hasSort ? MIN_SUBQUERIES : MAX_SUBQUERIES_SERVERLESS;
        } else {
            return MAX_SUBQUERIES;
        }
    }

    private static void verifyCircuitBreakingException(ResponseException re) throws IOException {
        Map<?, ?> map = responseAsMap(re.getResponse());
        assertMap(
            map,
            matchesMap().entry("status", 429).entry("error", matchesMap().extraOk().entry("type", "circuit_breaking_exception"))
        );
    }
}
