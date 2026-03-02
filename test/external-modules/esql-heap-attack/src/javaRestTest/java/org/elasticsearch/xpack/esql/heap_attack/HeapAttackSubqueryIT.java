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
 * Tests that run ESQL queries with subqueries that use a ton of memory. We want to make
 * sure they don't consume the entire heap and crash Elasticsearch.
 */
@TimeoutSuite(millis = 40 * TimeUnits.MINUTE)
public class HeapAttackSubqueryIT extends HeapAttackTestCase {

    // Reuse HeapAttackIT methods to prepare the indices
    private static final HeapAttackIT heapAttackIT = new HeapAttackIT();

    // the default number of subqueries used by this test, more than 2 subqueries OOMs in some tests
    private static final int DEFAULT_SUBQUERIES = 2;

    // the upper limit is defined in {@code Fork.MAX_BRANCHES}
    private static final int MAX_SUBQUERIES = 8;

    @Before
    public void checkCapability() {
        assumeTrue("Subquery is behind snapshot", Build.current().isSnapshot());
    }

    public void testManyKeywordFieldsWith10UniqueValuesInSubqueryIntermediateResults() throws IOException {
        heapAttackIT.initManyBigFieldsIndex(500, "keyword", false);
        for (int subquery : List.of(DEFAULT_SUBQUERIES, MAX_SUBQUERIES)) {
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
        int docs = 500;
        heapAttackIT.initManyBigFieldsIndex(docs, "keyword", true);
        for (int subquery : List.of(DEFAULT_SUBQUERIES, MAX_SUBQUERIES)) {
            assertCircuitBreaks(attempt -> buildSubqueries(subquery, "manybigfields"));
        }
    }

    public void testManyRandomKeywordFieldsInSubqueryIntermediateResultsWithSortOneField() throws IOException {
        // TODO OOM, TopN does not split large page in buildResult()
        int docs = 500;
        heapAttackIT.initManyBigFieldsIndex(docs, "keyword", true);
        for (int subquery : List.of(DEFAULT_SUBQUERIES, MAX_SUBQUERIES)) {
            assertCircuitBreaks(attempt -> buildSubqueriesWithSort(subquery, "manybigfields", " f000 "));
        }
    }

    public void testManyRandomKeywordFieldsInSubqueryIntermediateResultsWithSortManyFields() throws IOException {
        int docs = 500;
        heapAttackIT.initManyBigFieldsIndex(docs, "keyword", true);
        StringBuilder sortKeys = new StringBuilder();
        sortKeys.append("f000");
        for (int f = 1; f < 100; f++) {
            sortKeys.append(", f").append(String.format(Locale.ROOT, "%03d", f));
        }
        for (int subquery : List.of(DEFAULT_SUBQUERIES, MAX_SUBQUERIES)) {
            assertCircuitBreaks(attempt -> buildSubqueriesWithSort(subquery, "manybigfields", sortKeys.toString()));
        }
    }

    public void testManyRandomNumericFieldsInSubqueryIntermediateResultsWithSortManyFields() throws IOException {
        int docs = 1000;
        String type = randomFrom("integer", "long", "double");
        heapAttackIT.initManyBigFieldsIndex(docs, type, true);
        StringBuilder sortKeys = new StringBuilder();
        sortKeys.append("f000");
        for (int f = 1; f < 100; f++) {
            sortKeys.append(", f").append(String.format(Locale.ROOT, "%03d", f));
        }
        ListMatcher columns = matchesList();
        for (int f = 0; f < 1000; f++) {
            columns = columns.item(matchesMap().entry("name", "f" + String.format(Locale.ROOT, "%03d", f)).entry("type", type));
        }
        for (int subquery : List.of(MAX_SUBQUERIES)) {
            // results are returned from non-serverless environment, but CBE is expected in serverless
            try {
                Map<?, ?> response = buildSubqueriesWithSort(subquery, "manybigfields", sortKeys.toString());
                assertMap(response, matchesMap().entry("columns", columns));
            } catch (ResponseException e) {
                Map<?, ?> map = responseAsMap(e.getResponse());
                assertMap(
                    map,
                    matchesMap().entry("status", 429).entry("error", matchesMap().extraOk().entry("type", "circuit_breaking_exception"))
                );
            }
        }
    }

    public void testManyRandomTextFieldsInSubqueryIntermediateResults() throws IOException {
        int docs = 500;
        heapAttackIT.initManyBigFieldsIndex(docs, "text", true);
        for (int subquery : List.of(DEFAULT_SUBQUERIES, MAX_SUBQUERIES)) {
            assertCircuitBreaks(attempt -> buildSubqueries(subquery, "manybigfields"));
        }
    }

    public void testManyRandomTextFieldsInSubqueryIntermediateResultsWithSortOneField() throws IOException {
        // TODO OOM, different from sort with giant text field, VTopN does not split large page in buildResult(),
        // also ValuesFromManyReader doesn't honor jumboBytes
        int docs = 500;
        heapAttackIT.initManyBigFieldsIndex(docs, "text", true);
        for (int subquery : List.of(DEFAULT_SUBQUERIES, MAX_SUBQUERIES)) {
            assertCircuitBreaks(attempt -> buildSubqueriesWithSort(subquery, "manybigfields", " f000 "));
        }
    }

    public void testManyRandomTextFieldsInSubqueryIntermediateResultsWithSortManyFields() throws IOException {
        int docs = 500;
        heapAttackIT.initManyBigFieldsIndex(docs, "text", true);
        StringBuilder sortKeys = new StringBuilder();
        sortKeys.append("f000");
        for (int f = 1; f < 999; f++) {
            sortKeys.append(", f").append(String.format(Locale.ROOT, "%03d", f));
        }
        for (int subquery : List.of(DEFAULT_SUBQUERIES, MAX_SUBQUERIES)) {
            assertCircuitBreaks(attempt -> buildSubqueriesWithSort(subquery, "manybigfields", sortKeys.toString()));
        }
    }

    public void testManyKeywordFieldsWith10UniqueValuesInSubqueryIntermediateResultsWithAggNoGrouping() throws IOException {
        int docs = 500;
        heapAttackIT.initManyBigFieldsIndex(docs, "keyword", false);
        ListMatcher columns = matchesList().item(matchesMap().entry("name", "sum").entry("type", "long"));
        for (int subquery : List.of(DEFAULT_SUBQUERIES, MAX_SUBQUERIES)) {
            Map<?, ?> response = buildSubqueriesWithAgg(subquery, "manybigfields", "sum = SUM(LENGTH(f999))", null);
            ListMatcher values = matchesList();
            for (int i = 0; i < subquery; i++) {
                values = values.item(matchesList().item(1024 * docs));
            }
            assertMap(response, matchesMap().entry("columns", columns).entry("values", values));
        }
    }

    public void testManyRandomKeywordFieldsInSubqueryIntermediateResultsWithAggNoGrouping() throws IOException {
        int docs = 500;
        heapAttackIT.initManyBigFieldsIndex(docs, "keyword", true);
        ListMatcher columns = matchesList().item(matchesMap().entry("name", "sum").entry("type", "long"));
        for (int subquery : List.of(DEFAULT_SUBQUERIES, MAX_SUBQUERIES)) {
            Map<?, ?> response = buildSubqueriesWithAgg(subquery, "manybigfields", "sum = SUM(LENGTH(f999))", null);
            ListMatcher values = matchesList();
            for (int i = 0; i < subquery; i++) {
                values = values.item(matchesList().item(1024 * docs));
            }
            assertMap(response, matchesMap().entry("columns", columns).entry("values", values));
        }
    }

    public void testManyRandomKeywordFieldsInSubqueryIntermediateResultsWithAggWithGBYOneField() throws IOException {
        int docs = 500;
        heapAttackIT.initManyBigFieldsIndex(docs, "keyword", true);
        var columns = List.of(Map.of("name", "sum", "type", "long"), Map.of("name", "f000", "type", "keyword"));
        for (int subquery : List.of(DEFAULT_SUBQUERIES, MAX_SUBQUERIES)) {
            Map<?, ?> response = buildSubqueriesWithAgg(subquery, "manybigfields", "sum = SUM(LENGTH(f999))", "f000");
            var values = response.get("values");
            assertEquals(columns, response.get("columns"));
            assertTrue(values instanceof List<?> l && l.size() <= docs * subquery);
        }
    }

    public void testManyRandomKeywordFieldsInSubqueryIntermediateResultsWithAggGBYManyFields() throws IOException {
        int docs = 500;
        heapAttackIT.initManyBigFieldsIndex(docs, "keyword", true);
        StringBuilder grouping = new StringBuilder();
        grouping.append("f000");
        int groupBySize = 100;
        for (int f = 1; f < groupBySize; f++) {
            grouping.append(", f").append(String.format(Locale.ROOT, "%03d", f));
        }
        for (int subquery : List.of(MAX_SUBQUERIES)) {
            try {
                Map<?, ?> response = buildSubqueriesWithAgg(subquery, "manybigfields", "c = COUNT_DISTINCT(f999)", grouping.toString());
                assertTrue(response.get("columns") instanceof List<?> l && l.size() == (groupBySize + 1));
            } catch (ResponseException e) {
                Map<?, ?> map = responseAsMap(e.getResponse());
                assertMap(
                    map,
                    matchesMap().entry("status", 429).entry("error", matchesMap().extraOk().entry("type", "circuit_breaking_exception"))
                );
            }
        }
    }

    public void testGiantTextFieldInSubqueryIntermediateResults() throws IOException {
        int docs = 50;
        heapAttackIT.initGiantTextField(docs, false, 5);
        for (int subquery : List.of(DEFAULT_SUBQUERIES, MAX_SUBQUERIES)) {
            assertCircuitBreaks(attempt -> buildSubqueries(subquery, "bigtext"));
        }
    }

    public void testGiantTextFieldInSubqueryIntermediateResultsWithSort() throws IOException {
        // TODO OOM, after pages are added to TopN, the page is released, so the overestimation on big blocks are gone
        int docs = 50;
        heapAttackIT.initGiantTextField(docs, false, 5);
        for (int subquery : List.of(DEFAULT_SUBQUERIES, MAX_SUBQUERIES)) {
            assertCircuitBreaks(attempt -> buildSubqueriesWithSort(subquery, "bigtext", " f "));
        }
    }

    public void testGiantTextFieldInSubqueryIntermediateResultsWithAggNoGrouping() throws IOException {
        int docs = 100;
        heapAttackIT.initGiantTextField(docs, false, 5);
        ListMatcher columns = matchesList().item(matchesMap().entry("name", "sum").entry("type", "long"));
        for (int subquery : List.of(DEFAULT_SUBQUERIES, MAX_SUBQUERIES)) {
            Map<?, ?> response = buildSubqueriesWithAgg(subquery, "bigtext", "sum = SUM(LENGTH(f))", null);
            ListMatcher values = matchesList();
            for (int i = 0; i < subquery; i++) {
                values = values.item(matchesList().item(1024 * 1024 * 5 * docs));
            }
            assertMap(response, matchesMap().entry("columns", columns).entry("values", values));
        }
    }

    private Map<String, Object> buildSubqueries(int subqueries, String indexName) throws IOException {
        StringBuilder query = startQuery();
        String subquery = "(FROM " + indexName + " )";
        query.append("FROM ").append(subquery);
        for (int i = 1; i < subqueries; i++) {
            query.append(", ").append(subquery);
        }
        // the limit should not be necessary, it is just to limit the result size for giant text test temporarily
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
        // the limit is added to avoid unbounded sort
        subquery.append("(FROM ").append(indexName).append(" | SORT ").append(sortKeys).append(" )");
        query.append("FROM ").append(subquery);
        for (int i = 1; i < subqueries; i++) {
            query.append(", ").append(subquery);
        }
        query.append(" \"}");
        return responseAsMap(query(query.toString(), "columns"));
    }
}
