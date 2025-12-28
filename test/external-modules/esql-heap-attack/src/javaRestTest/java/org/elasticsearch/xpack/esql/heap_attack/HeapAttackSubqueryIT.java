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
import org.elasticsearch.test.ListMatcher;

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

    // the default number of subqueries used by this test
    private static final int DEFAULT_SUBQUERIES = 2;

    // the upper limit is defined in {@code Fork.MAX_BRANCHES}
    private static final int MAX_SUBQUERIES = 8;

    /*
     * The index's size is 1MB * 500, with only 10 unique keyword values. and these queries don't have aggregation or sort.
     * CBE is not expected to be triggered here.
     */
    public void testManyKeywordFieldsWith10UniqueValuesInSubqueryIntermediateResults() throws IOException {
        assumeTrue("Subquery is behind snapshot", Build.current().isSnapshot());
        heapAttackIT.initManyBigFieldsIndex(500, "keyword", false);
        ListMatcher columns = matchesList();
        for (int f = 0; f < 1000; f++) {
            columns = columns.item(matchesMap().entry("name", "f" + String.format(Locale.ROOT, "%03d", f)).entry("type", "keyword"));
        }
        for (int subquery : List.of(DEFAULT_SUBQUERIES, MAX_SUBQUERIES)) {
            Map<?, ?> response = buildSubqueries(subquery, "manybigfields");
            assertMap(response, matchesMap().entry("columns", columns));
        }
    }

    /*
     * The index's size is 1MB * 500, each field has 500 unique/random keyword values, and these queries don't have aggregation or sort.
     * CBE is not expected to be triggered here.
     */
    public void testManyRandomKeywordFieldsInSubqueryIntermediateResults() throws IOException {
        assumeTrue("Subquery is behind snapshot", Build.current().isSnapshot());
        // 500MB random/unique keyword values trigger CBE, should not OOM
        heapAttackIT.initManyBigFieldsIndex(500, "keyword", true);
        // 2 subqueries are enough to trigger CBE
        assertCircuitBreaks(attempt -> buildSubqueries(DEFAULT_SUBQUERIES, "manybigfields"));
    }

    /*
     * The index's size is 1MB * 500, with only 10 unique keyword values. and these queries have aggregation without grouping.
     * CBE is not expected to be triggered here.
     */
    public void testManyKeywordFieldsWith10UniqueValuesInSubqueryIntermediateResultsWithAggNoGrouping() throws IOException {
        assumeTrue("Subquery is behind snapshot", Build.current().isSnapshot());
        int docs = 500; // 10 unique keyword values do not trigger CBE
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

    /*
     * The index's size is 1MB * 500, each field has 500 unique/random keyword values, and these queries have aggregation without grouping.
     * CBE is not expected to be triggered here.
     */
    public void testManyRandomKeywordFieldsInSubqueryIntermediateResultsWithAggNoGrouping() throws IOException {
        assumeTrue("Subquery is behind snapshot", Build.current().isSnapshot());
        int docs = 500; // 500MB random/unique keyword values
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

    /*
     * The index's size is 1MB * 500, each field has 500 unique/random keyword values.
     * And these queries have aggregation with grouping by one field, there are 500 buckets.
     * This is mainly to test HashAggregationOperator, CBE is not expected to be triggered here.
     */
    public void testManyRandomKeywordFieldsInSubqueryIntermediateResultsWithAggWithGrouping500Buckets() throws IOException {
        assumeTrue("Subquery is behind snapshot", Build.current().isSnapshot());
        int docs = 500; // 500MB random/unique keyword values
        heapAttackIT.initManyBigFieldsIndex(docs, "keyword", true);
        // grouping by f000 which has about 500 unique buckets/values
        var columns = List.of(Map.of("name", "sum", "type", "long"), Map.of("name", "f000", "type", "keyword"));
        for (int subquery : List.of(DEFAULT_SUBQUERIES, MAX_SUBQUERIES)) {
            Map<?, ?> response = buildSubqueriesWithAgg(subquery, "manybigfields", "sum = SUM(LENGTH(f999))", "f000");
            var values = response.get("values");
            assertEquals(columns, response.get("columns"));
            assertTrue(values instanceof List<?> l && l.size() <= docs * subquery);
        }
    }

    /*
     * The index's size is 1MB * 500, each field has 500 unique/random keyword values.
     * And these queries have aggregation with grouping by 999 fields, there are 500 * 999 buckets.
     * This is mainly to test HashAggregationOperator, CBE is not expected to be triggered here.
     */
    public void testManyRandomKeywordFieldsInSubqueryIntermediateResultsWithAggWithLargeGrouping() throws IOException {
        assumeTrue("Subquery is behind snapshot", Build.current().isSnapshot());
        int docs = 500; // 500MB random/unique keyword values
        heapAttackIT.initManyBigFieldsIndex(docs, "keyword", true);
        // grouping by 500 * 999 unique buckets/values
        StringBuilder grouping = new StringBuilder();
        grouping.append("f000");
        for (int f = 1; f < 999; f++) {
            grouping.append(", f").append(String.format(Locale.ROOT, "%03d", f));
        }
        for (int subquery : List.of(DEFAULT_SUBQUERIES, MAX_SUBQUERIES)) {
            if (subquery == MAX_SUBQUERIES) {
                continue; // TODO 8 subqueries cause OOM, investigation needed
            }
            assertCircuitBreaks(
                attempt -> buildSubqueriesWithAgg(subquery, "manybigfields", "c = COUNT_DISTINCT(f999)", grouping.toString())
            );
        }
    }

    /*
     * The index's size is 1MB * 500, each field has 500 unique/random keyword values.
     * And these queries have sort on one field, there are 500 distinct values, each value is 1KB.
     * This is mainly to test TopNOperator, CBE is not expected to be triggered here.
     */
    public void testManyRandomKeywordFieldsInSubqueryIntermediateResultsWithSortOneField() throws IOException {
        assumeTrue("Subquery is behind snapshot", Build.current().isSnapshot());
        int docs = 500; // 500MB random/unique keyword values
        heapAttackIT.initManyBigFieldsIndex(docs, "keyword", true);
        for (int subquery : List.of(DEFAULT_SUBQUERIES, MAX_SUBQUERIES)) {
            if (subquery == MAX_SUBQUERIES) {
                continue; // TODO 8 subqueries cause OOM, investigation needed
            }
            assertCircuitBreaks(attempt -> buildSubqueriesWithSort(subquery, "manybigfields", " f000 ", docs));
        }
    }

    /*
     * The index's size is 1MB * 500, each field has 500 unique/random keyword values.
     * And these queries have sort on 999 fields, there are 500 * 999 distinct values, each value is 1KB.
     * This is mainly to test TopNOperator, CBE is not expected to be triggered here.
     */
    public void testManyRandomKeywordFieldsInSubqueryIntermediateResultsWithSort500Fields() throws IOException {
        assumeTrue("skip this test for now, it OOM", false);
        assumeTrue("Subquery is behind snapshot", Build.current().isSnapshot());
        int docs = 500; // 500MB random/unique keyword values
        heapAttackIT.initManyBigFieldsIndex(docs, "keyword", true);
        // order by 500 * 999 unique values
        StringBuilder sortKeys = new StringBuilder();
        sortKeys.append("f000");
        for (int f = 1; f < 999; f++) {
            sortKeys.append(", f").append(String.format(Locale.ROOT, "%03d", f));
        }
        for (int subquery : List.of(DEFAULT_SUBQUERIES, MAX_SUBQUERIES)) {
            if (subquery == MAX_SUBQUERIES) {
                continue; // TODO even 2 subqueries cause OOM, investigation needed
            }
            assertCircuitBreaks(attempt -> buildSubqueriesWithSort(subquery, "manybigfields", sortKeys.toString(), docs));
        }

    }

    public void testGiantTextFieldInSubqueryIntermediateResults() throws IOException {
        assumeTrue("skip this test for now, it OOM", false);
        assumeTrue("Subquery is behind snapshot", Build.current().isSnapshot());
        heapAttackIT.initGiantTextField(100);
        for (int subquery : List.of(DEFAULT_SUBQUERIES, MAX_SUBQUERIES)) {
            // TODO should CBE, it OOM with 2 subqueries
            Map<?, ?> response = buildSubqueries(subquery, "bigtext");
            ListMatcher columns = matchesList().item(matchesMap().entry("name", "f").entry("type", "text"));
            assertMap(response, matchesMap().entry("columns", columns));
        }
    }

    public void testGiantTextFieldInSubqueryIntermediateResultsWithAggNoGrouping() throws IOException {
        assumeTrue("Subquery is behind snapshot", Build.current().isSnapshot());
        int docs = 100;
        heapAttackIT.initGiantTextField(docs);
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

    private Map<String, Object> buildSubqueriesWithSort(int subqueries, String indexName, String sortKeys, int rows) throws IOException {
        StringBuilder query = startQuery();
        StringBuilder subquery = new StringBuilder();
        subquery.append("(FROM ").append(indexName).append(" | SORT ").append(sortKeys).append(" | LIMIT ").append(rows).append(" )");
        query.append("FROM ").append(subquery);
        for (int i = 1; i < subqueries; i++) {
            query.append(", ").append(subquery);
        }
        query.append(" \"}");
        return responseAsMap(query(query.toString(), "columns,values"));
    }
}
