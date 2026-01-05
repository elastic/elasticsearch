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
            Map<?, ?> response = buildSubqueries(subquery, "manybigfields", "");
            assertMap(response, matchesMap().entry("columns", columns));
        }
    }

    /*
     * The index's size is 1MB * 500, each field has 500 unique/random keyword values, and these queries don't have aggregation or sort.
     * CBE is not triggered here.
     */
    public void testManyRandomKeywordFieldsInSubqueryIntermediateResults() throws IOException {
        assumeTrue("Subquery is behind snapshot", Build.current().isSnapshot());
        // 500MB random/unique keyword values trigger CBE, should not OOM
        heapAttackIT.initManyBigFieldsIndex(500, "keyword", true);
        // 2 subqueries are enough to trigger CBE, confirmed where this CBE happens in ExchangeService.doFetchPageAsync,
        // as a few big pages are loaded into the exchange buffer
        assertCircuitBreaks(attempt -> buildSubqueries(DEFAULT_SUBQUERIES, "manybigfields", ""));
        // TODO 8 subqueries OOM, because the memory consumed by lucene is not properly tracked in ValuesSourceReaderOperator yet.
        // Lucene90DocValuesProducer are on the top of objects list, also BlockSourceReader.scratch is not tracked by circuit breaker yet,
        // skip 8 subqueries for now
        // assertCircuitBreaks(attempt -> buildSubqueries(MAX_SUBQUERIES, "manybigfields", ""));
    }

    /*
     * The index's size is 1MB * 500, with only 10 unique keyword values. and these queries have aggregation without grouping.
     * CBE is not triggered here.
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
     * CBE is not triggered here.
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
     * This is mainly to test HashAggregationOperator, CBE is not triggered here.
     */
    public void testManyRandomKeywordFieldsInSubqueryIntermediateResultsWithAggWithGBYOneField() throws IOException {
        assumeTrue("Subquery is behind snapshot", Build.current().isSnapshot());
        int docs = 500; // 500MB random/unique keyword values
        heapAttackIT.initManyBigFieldsIndex(docs, "keyword", true);
        // group by f000 which has about 500 unique buckets/values, group by 50 fields works fine for 8 subqueries as well
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
     * And these queries have aggregation with grouping by 100 fields, there are 500 * 100 buckets.
     * This is mainly to test HashAggregationOperator and ValuesSourceReaderOperator, CBE is expected to be triggered here.
     */
    public void testManyRandomKeywordFieldsInSubqueryIntermediateResultsWithAggGBYManyFields() throws IOException {
        assumeTrue("Subquery is behind snapshot", Build.current().isSnapshot());
        int docs = 500; // 500MB random/unique keyword values
        heapAttackIT.initManyBigFieldsIndex(docs, "keyword", true);
        // Some data points:
        // 1. group by 50 fields completes successfully for 2/8 subqueries, there is no CBE
        // 2. group by 100 fields with 2 subqueries completes successfully, 8 subqueries triggers CBE
        // 3. group by 500 fields, HashAggregationOperator.addInput triggers CBE for 2 subqueries,
        // TODO 8 subqueries trigger OOM, skip group by 500 fields with 8 subqueries,
        // the walkaround that prevents the OOM is setting FIELD_EXTRACT_PREFERENCE=STORED
        StringBuilder grouping = new StringBuilder();
        grouping.setLength(0);
        grouping.append("f000");
        for (int f = 1; f < 100; f++) {
            grouping.append(", f").append(String.format(Locale.ROOT, "%03d", f));
        }
        Map<?, ?> response = buildSubqueriesWithAgg(DEFAULT_SUBQUERIES, "manybigfields", "c = COUNT_DISTINCT(f999)", grouping.toString());
        var columns = response.get("columns");
        assertTrue(columns instanceof List<?> l && l.size() == 101);
        assertCircuitBreaks(
            attempt -> buildSubqueriesWithAgg(MAX_SUBQUERIES, "manybigfields", "c = COUNT_DISTINCT(f999)", grouping.toString())
        );
    }

    /*
     * The index's size is 1MB * 500, each field has 500 unique/random keyword values.
     * And these queries have sort on one field, there are 500 distinct values, each value is 1KB.
     * This is mainly to test TopNOperator, addInput triggers CBE.
     */
    public void testManyRandomKeywordFieldsInSubqueryIntermediateResultsWithSortOneField() throws IOException {
        assumeTrue("Subquery is behind snapshot", Build.current().isSnapshot());
        int docs = 500; // 500MB random/unique keyword values
        heapAttackIT.initManyBigFieldsIndex(docs, "keyword", true);
        for (int subquery : List.of(DEFAULT_SUBQUERIES, MAX_SUBQUERIES)) {
            assertCircuitBreaks(attempt -> buildSubqueriesWithSort(subquery, "manybigfields", " f000 ", docs));
        }
    }

    /*
     * The index's size is 1MB * 500, each field has 500 unique/random keyword values.
     * And these queries have sort on 100 fields, there are 500 * 100 distinct values, each value is 1KB.
     * This is mainly to test TopNOperator.
     */
    public void testManyRandomKeywordFieldsInSubqueryIntermediateResultsWithSortManyFields() throws IOException {
        assumeTrue("Subquery is behind snapshot", Build.current().isSnapshot());
        int docs = 500; // // 500MB random/unique keyword values
        heapAttackIT.initManyBigFieldsIndex(docs, "keyword", true);
        // Some data points:
        // 1. Sort on 999 fields, with 500 * 999 random values, without subquery fail/OOM in lucene, LeafFieldComparator
        // 2. Sort on 20 fields(500*20 random values), 2 subqueries trigger CBE, 8 subqueries trigger OOM, haven't found a walkaround yet.
        StringBuilder sortKeys = new StringBuilder();
        sortKeys.append("f000");
        for (int f = 1; f < 100; f++) {
            sortKeys.append(", f").append(String.format(Locale.ROOT, "%03d", f));
        }
        // TODO skip 8 subqueries with sort 100 fields, as it OOMs, seems like the constrain is in reading data from lucene,
        // LuceneTopNSourceOperator.NonScoringPerShardCollector is the main memory consumer,
        // MultiLeafFieldComparator seems big but it is only about 15% of the size of NonScoringPerShardCollector,
        for (int subquery : List.of(DEFAULT_SUBQUERIES)) {
            assertCircuitBreaks(attempt -> buildSubqueriesWithSort(subquery, "manybigfields", sortKeys.toString(), docs));
        }
    }

    // It is likely the lack of memory tracking for OutputOperator cause OOM instead of CBE here.
    public void testGiantTextFieldInSubqueryIntermediateResults() throws IOException {
        assumeTrue("Subquery is behind snapshot", Build.current().isSnapshot());
        // OOM heap dump shows around 45 docs/pages in it locally, which means fetching 45 docs will cause OOM, each page is 5MB.
        // 2 subqueries and 8 subqueries both OOM, however they have different patterns in the heap dump.
        // 1. According to the heap dump of 2 subqueries, the OOM happened during adding page into OutputOperator for returning results,
        // it seems like there is no memory tracking for OutputOperator, so CBE won't happen to this test, the constrain is in returning
        // data, limiting to returning 30 docs reduces the final OutputOperator size and avoids OOM for 2 subqueries.
        // 2. According to the heap dump of 8 subqueries, the pattern is very similar as OOM of 8 subqueries in
        // testManyRandomKeywordFieldsInSubqueryIntermediateResults, BlockSourceReader.scratch is not tracked by CB,
        // the constrain is in reading data.
        // TODO improve memory tracking in OutputOperator and BlockSourceReader.scratch
        int docs = 40; // 40 docs *5MB does not OOM without subquery, 2 or 8 subqueries OOM with different patterns in the heap dump
        int limit = 30;
        heapAttackIT.initGiantTextField(docs);
        for (int subquery : List.of(DEFAULT_SUBQUERIES)) {
            Map<?, ?> response = buildSubqueries(subquery, "bigtext", " | limit " + limit);
            // Map<?, ?> response = buildSubqueries(subquery, "bigtext", "");
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

    private Map<String, Object> buildSubqueries(int subqueries, String indexName, String limit) throws IOException {
        StringBuilder query = startQuery();
        String subquery = "(FROM " + indexName + " )";
        query.append("FROM ").append(subquery);
        for (int i = 1; i < subqueries; i++) {
            query.append(", ").append(subquery);
        }
        query.append(limit).append(" \"}");
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
