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

    private static final int MAX_STRING_FIELDS = 1000;

    private static final int MAX_STRING_FIELD_SERVERLESS = 700;

    private static final int MAX_DOC = 200;

    private static final int MAX_DOC_SERVERLESS = 100;

    @Before
    public void checkCapability() {
        assumeTrue("Subquery is behind snapshot", Build.current().isSnapshot());
    }

    public void testManyKeywordFieldsWith10UniqueValuesInSubqueryIntermediateResults() throws IOException {
        heapAttackIT.initManyBigFieldsIndex(MAX_DOC, "keyword", false, MAX_STRING_FIELDS);
        for (int subquery : List.of(MIN_SUBQUERIES, MAX_SUBQUERIES)) {
            ListMatcher columns = matchesList();
            int fieldsToRead = subquery < MAX_SUBQUERIES ? 1000 : 600; // with 1000 fields we circuit break
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
        heapAttackIT.initManyBigFieldsIndex(docs, "keyword", true, fields());
        assertCircuitBreaks(attempt -> buildSubqueries(maxSubqueries(), "manybigfields"));
    }

    public void testManyRandomKeywordFieldsInSubqueryIntermediateResultsWithSortOneField() throws IOException {
        int docs = docs();
        heapAttackIT.initManyBigFieldsIndex(docs, "keyword", true, fields());
        int subqueries = isServerless() ? MIN_SUBQUERIES : MAX_SUBQUERIES;
        ListMatcher columns = matchesList();
        for (int f = 0; f < fields(); f++) {
            columns = columns.item(matchesMap().entry("name", "f" + String.format(Locale.ROOT, "%03d", f)).entry("type", "keyword"));
        }
        // serverless behaves differently from stateful
        try {
            Map<?, ?> response = buildSubqueriesWithSort(subqueries, "manybigfields", "f000");
            assertMap(response, matchesMap().entry("columns", columns));
        } catch (ResponseException e) {
            Map<?, ?> map = responseAsMap(e.getResponse());
            assertMap(
                map,
                matchesMap().entry("status", 429).entry("error", matchesMap().extraOk().entry("type", "circuit_breaking_exception"))
            );
        }
    }

    public void testManyRandomKeywordFieldsInSubqueryIntermediateResultsWithSortManyFields() throws IOException {
        int docs = docs();
        heapAttackIT.initManyBigFieldsIndex(docs, "keyword", true, fields());
        StringBuilder sortKeys = new StringBuilder();
        sortKeys.append("f000");
        for (int f = 1; f < 11; f++) {
            sortKeys.append(", f").append(String.format(Locale.ROOT, "%03d", f));
        }
        int subqueries = isServerless() ? MIN_SUBQUERIES : MAX_SUBQUERIES;
        ListMatcher columns = matchesList();
        for (int f = 0; f < fields(); f++) {
            columns = columns.item(matchesMap().entry("name", "f" + String.format(Locale.ROOT, "%03d", f)).entry("type", "keyword"));
        }
        // serverless behaves differently from stateful
        try {
            Map<?, ?> response = buildSubqueriesWithSort(subqueries, "manybigfields", sortKeys.toString());
            assertMap(response, matchesMap().entry("columns", columns));
        } catch (ResponseException e) {
            Map<?, ?> map = responseAsMap(e.getResponse());
            assertMap(
                map,
                matchesMap().entry("status", 429).entry("error", matchesMap().extraOk().entry("type", "circuit_breaking_exception"))
            );
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
            Map<?, ?> map = responseAsMap(e.getResponse());
            assertMap(
                map,
                matchesMap().entry("status", 429).entry("error", matchesMap().extraOk().entry("type", "circuit_breaking_exception"))
            );
        }
    }

    public void testManyRandomTextFieldsInSubqueryIntermediateResults() throws IOException {
        int docs = docs();
        heapAttackIT.initManyBigFieldsIndex(docs, "text", true, fields());
        assertCircuitBreaks(attempt -> buildSubqueries(maxSubqueries(), "manybigfields"));
    }

    public void testManyRandomTextFieldsInSubqueryIntermediateResultsWithSortOneField() throws IOException {
        int docs = docs();
        heapAttackIT.initManyBigFieldsIndex(docs, "text", true, fields());
        assertCircuitBreaks(attempt -> buildSubqueriesWithSort(maxSubqueries(), "manybigfields", " substring(f000, 5) "));
    }

    public void testManyRandomTextFieldsInSubqueryIntermediateResultsWithSortManyFields() throws IOException {
        int docs = docs();
        heapAttackIT.initManyBigFieldsIndex(docs, "text", true, fields());
        StringBuilder sortKeys = new StringBuilder();
        sortKeys.append(" substring(f000, 5) ");
        for (int f = 1; f < 5; f++) {
            sortKeys.append(", substring(f").append(String.format(Locale.ROOT, "%03d", f)).append(", 5) ");
        }
        assertCircuitBreaks(attempt -> buildSubqueriesWithSort(maxSubqueries(), "manybigfields", sortKeys.toString()));
    }

    public void testManyKeywordFieldsWith10UniqueValuesInSubqueryIntermediateResultsWithAggNoGrouping() throws IOException {
        int docs = docs();
        heapAttackIT.initManyBigFieldsIndex(docs, "keyword", false, MAX_STRING_FIELDS);
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
        heapAttackIT.initManyBigFieldsIndex(docs, "keyword", true, MAX_STRING_FIELDS);
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
        heapAttackIT.initManyBigFieldsIndex(docs, "keyword", true, MAX_STRING_FIELDS);
        var columns = List.of(Map.of("name", "sum", "type", "long"), Map.of("name", "f000", "type", "keyword"));
        Map<?, ?> response = buildSubqueriesWithAgg(MAX_SUBQUERIES, "manybigfields", "sum = SUM(LENGTH(f999))", "f000");
        var values = response.get("values");
        assertEquals(columns, response.get("columns"));
        assertTrue(values instanceof List<?> l && l.size() <= docs * MAX_SUBQUERIES);
    }

    public void testManyRandomKeywordFieldsInSubqueryIntermediateResultsWithAggGBYManyFields() throws IOException {
        int docs = docs();
        heapAttackIT.initManyBigFieldsIndex(docs, "keyword", true, fields());
        StringBuilder grouping = new StringBuilder();
        grouping.append("f000");
        int groupBySize = 100;
        for (int f = 1; f < groupBySize; f++) {
            grouping.append(", f").append(String.format(Locale.ROOT, "%03d", f));
        }
        try {
            Map<?, ?> response = buildSubqueriesWithAgg(maxSubqueries(), "manybigfields", "c = COUNT_DISTINCT(f499)", grouping.toString());
            assertTrue(response.get("columns") instanceof List<?> l && l.size() == (groupBySize + 1));
        } catch (ResponseException e) {
            Map<?, ?> map = responseAsMap(e.getResponse());
            assertMap(
                map,
                matchesMap().entry("status", 429).entry("error", matchesMap().extraOk().entry("type", "circuit_breaking_exception"))
            );
        }
    }

    public void testGiantTextFieldInSubqueryIntermediateResults() throws IOException {
        int docs = 20;
        heapAttackIT.initGiantTextField(docs, false, 5);
        assertCircuitBreaks(attempt -> buildSubqueries(maxSubqueries(), "bigtext"));
    }

    public void testGiantTextFieldInSubqueryIntermediateResultsWithSort() throws IOException {
        int docs = 20;
        heapAttackIT.initGiantTextField(docs, false, 5);
        assertCircuitBreaks(attempt -> buildSubqueriesWithSort(maxSubqueries(), "bigtext", " substring(f, 5) "));
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
        heapAttackIT.initManyBigFieldsIndex(docs, "keyword", false, MAX_STRING_FIELDS);
        verifyLoadDocSequenceResults(docs, "keyword");
    }

    public void testLoadDocSequenceReturnsCorrectResultsText() throws IOException {
        int docs = 20;
        heapAttackIT.initManyBigFieldsIndex(docs, "text", false, MAX_STRING_FIELDS);
        verifyLoadDocSequenceResults(docs, "text");
    }

    private void verifyLoadDocSequenceResults(int docs, String dataType) throws IOException {
        List<Map<String, String>> columns = new ArrayList<>();
        for (int f = 0; f < MAX_STRING_FIELDS; f++) {
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
            Map<?, ?> map = responseAsMap(e.getResponse());
            assertMap(
                map,
                matchesMap().entry("status", 429).entry("error", matchesMap().extraOk().entry("type", "circuit_breaking_exception"))
            );
        }
    }

    private Map<String, Object> buildSubqueries(int subqueries, String indexName) throws IOException {
        StringBuilder query = startQuery();
        String subquery = "(FROM " + indexName + " )";
        query.append("FROM ").append(subquery);
        for (int i = 1; i < subqueries; i++) {
            query.append(", ").append(subquery);
        }
        query.append(" \"}");
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
        query.append(" \"}");
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
        query.append(" \"}");
        return responseAsMap(query(query.toString(), "columns"));
    }

    private Map<String, Object> buildSubqueriesWithSortInMainQuery(int subqueries, String indexName, String sortKeys) throws IOException {
        StringBuilder query = startQuery();
        String subquery = "(FROM " + indexName + " )";
        query.append("FROM ").append(subquery);
        for (int i = 1; i < subqueries; i++) {
            query.append(", ").append(subquery);
        }
        query.append(" | SORT ").append(sortKeys).append(" \"}");
        return responseAsMap(query(query.toString(), "columns, values"));
    }

    private static int docs() throws IOException {
        // serverless has 6 shards, non-serverless has 1 shard,
        // limiting the number of docs to reduce the gc lagging and intermittent OOMs in serverless
        return isServerless() ? MAX_DOC_SERVERLESS : MAX_DOC;
    }

    private static int fields() throws IOException {
        // serverless has 6 shards, non-serverless has 1 shard,
        // limiting the number of keyword/text fields to reduce the gc lagging and intermittent OOMs in serverless
        return isServerless() ? MAX_STRING_FIELD_SERVERLESS : MAX_STRING_FIELDS;
    }

    private static int maxSubqueries() throws IOException {
        // serverless has 6 shards, non-serverless has 1 shard, the number of exchange operators increases when the number of subqueries
        // increase, limiting the number of subqueries to reduce the gc lagging and intermittent OOMs in serverless
        return isServerless() ? MAX_SUBQUERIES_SERVERLESS : MAX_SUBQUERIES;
    }
}
