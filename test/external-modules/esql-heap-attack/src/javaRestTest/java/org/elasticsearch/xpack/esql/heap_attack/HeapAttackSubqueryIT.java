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

    /*
     * The index's size is 1MB * 500, with only 10 unique keyword values. and these queries don't have aggregation or sort.
     * CBE is not expected to be triggered here.
     */
    public void testManyKeywordFieldsWith10UniqueValuesInSubqueryIntermediateResults() throws IOException {
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
        // 500MB random/unique keyword values trigger CBE, should not OOM
        int docs = 500;
        heapAttackIT.initManyBigFieldsIndex(docs, "keyword", true);
        for (int subquery : List.of(DEFAULT_SUBQUERIES, MAX_SUBQUERIES)) {
            assertCircuitBreaks(attempt -> buildSubqueries(subquery, "manybigfields", ""));
        }
    }

    /*
     * The index's size is 1MB * 500, each field has 500 unique/random keyword values.
     * And these queries have sort on one field, there are 500 distinct values, each value is 1KB.
     * This is mainly to test TopNOperator, addInput triggers CBE.
     */
    public void testManyRandomKeywordFieldsInSubqueryIntermediateResultsWithSortOneField() throws IOException {
        if (isServerless()) { // 500 docs OOM in serverless
            return;
        }
        int docs = 500; // 500MB random/unique keyword values
        heapAttackIT.initManyBigFieldsIndex(docs, "keyword", true);
        for (int subquery : List.of(DEFAULT_SUBQUERIES, MAX_SUBQUERIES)) {
            assertCircuitBreaks(attempt -> buildSubqueriesWithSort(subquery, "manybigfields", " f000 "));
        }
    }

    /*
     * The index's size is 1MB * 500, each field has 500 unique/random keyword values.
     * And these queries have sort on 100 fields, there are 500 * 100 distinct values, each value is 1KB.
     * This is mainly to test TopNOperator.
     */
    public void testManyRandomKeywordFieldsInSubqueryIntermediateResultsWithSortManyFields() throws IOException {
        if (isServerless()) { // both 100 and 500 docs OOM in serverless
            return;
        }
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
            assertCircuitBreaks(attempt -> buildSubqueriesWithSort(subquery, "manybigfields", sortKeys.toString()));
        }
    }

    /*
     * The index's size is 1MB * 500, each field has 500 unique/random text values, and these queries don't have aggregation or sort.
     * CBE is not triggered here.
     */
    public void testManyRandomTextFieldsInSubqueryIntermediateResults() throws IOException {
        if (isServerless()) {
            return;
        }
        // 500MB random/unique keyword values trigger CBE, should not OOM
        // serverless CI does not OOM or CB with 100 docs.
        int docs = 500;
        heapAttackIT.initManyBigFieldsIndex(docs, "text", true);
        // 2 subqueries are enough to trigger CBE, confirmed where this CBE happens in ExchangeService.doFetchPageAsync,
        // as a few big pages are loaded into the exchange buffer
        // TODO 8 subqueries OOM, because the memory consumed by lucene is not properly tracked in ValuesSourceReaderOperator yet.
        // Lucene90DocValuesProducer are on the top of objects list, also BlockSourceReader.scratch is not tracked by circuit breaker yet,
        // skip 8 subqueries for now
        for (int subquery : List.of(DEFAULT_SUBQUERIES)) {
            assertCircuitBreaks(attempt -> buildSubqueries(subquery, "manybigfields", ""));
        }
    }

    /*
     * The index's size is 1MB * 500, each field has 500 unique/random text values.
     * And these queries have sort on one field, there are 500 distinct values, each value is 1KB.
     * This is mainly to test TopNOperator, addInput triggers CBE.
     */
    public void testManyRandomTextFieldsInSubqueryIntermediateResultsWithSortOneField() throws IOException {
        if (isServerless()) {
            return;
        }
        int docs = 500; // 500MB random/unique keyword values
        heapAttackIT.initManyBigFieldsIndex(docs, "text", true);
        // the sort of text field is not pushed to lucene, different from keyword, this test should CB
        // TODO 8 subqueries OOMs during ValuesSourceReaderOperator, similar to no sort case, skip it for now
        for (int subquery : List.of(DEFAULT_SUBQUERIES)) {
            assertCircuitBreaks(attempt -> buildSubqueriesWithSort(subquery, "manybigfields", " f000 "));
        }
    }

    /*
     * The index's size is 1MB * 500, each field has 500 unique/random text values.
     * And these queries have sort on 100 fields, there are 500 * 100 distinct values, each value is 1KB.
     * This is mainly to test TopNOperator.
     */
    public void testManyRandomTextFieldsInSubqueryIntermediateResultsWithSortManyFields() throws IOException {
        if (isServerless()) { // both 100 and 500 docs OOM in serverless
            return;
        }
        int docs = 500; // // 500MB random/unique keyword values
        heapAttackIT.initManyBigFieldsIndex(docs, "text", true);
        StringBuilder sortKeys = new StringBuilder();
        sortKeys.append("f000");
        for (int f = 1; f < 999; f++) {
            sortKeys.append(", f").append(String.format(Locale.ROOT, "%03d", f));
        }
        // the sort of text field is not pushed to lucene, different from keyword, this test should CB
        // TODO 8 subqueries OOMs during ValuesSourceReaderOperator, similar to no sort case, skip it for now
        for (int subquery : List.of(DEFAULT_SUBQUERIES)) {
            assertCircuitBreaks(attempt -> buildSubqueriesWithSort(subquery, "manybigfields", sortKeys.toString()));
        }
    }

    /*
     * The index's size is 1MB * 500, with only 10 unique keyword values. and these queries have aggregation without grouping.
     * CBE is not triggered here.
     */
    public void testManyKeywordFieldsWith10UniqueValuesInSubqueryIntermediateResultsWithAggNoGrouping() throws IOException {
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
        if (isServerless()) { // skip this test in serverless, as it OOMs with 100 fields and 2 subqueries
            return;
        }
        // GBY 100 fields CB and 500 fields docs OOM with 2 subqueries
        int docs = 500; // 500MB random/unique keyword values
        heapAttackIT.initManyBigFieldsIndex(docs, "keyword", true);
        // Some data points:
        // 1. group by 50 fields completes successfully for 2/8 subqueries, there is no CBE
        // 2. group by 100 fields with 2 subqueries completes successfully, 8 subqueries triggers CBE
        // 3. group by 500 fields, HashAggregationOperator.addInput triggers CBE for 2 subqueries locally, however it OOMs in serverless CI,
        // TODO 8 subqueries trigger OOM, skip group by 500 fields with 8 subqueries,
        // the walkaround that prevents the OOM is setting FIELD_EXTRACT_PREFERENCE=STORED
        StringBuilder grouping = new StringBuilder();
        grouping.append("f000");
        int groupBySize = 100;
        for (int f = 1; f < groupBySize; f++) {
            grouping.append(", f").append(String.format(Locale.ROOT, "%03d", f));
        }
        Map<?, ?> response = buildSubqueriesWithAgg(DEFAULT_SUBQUERIES, "manybigfields", "c = COUNT_DISTINCT(f999)", grouping.toString());
        assertTrue(response.get("columns") instanceof List<?> l && l.size() == (groupBySize + 1));
        assertCircuitBreaks(
            attempt -> buildSubqueriesWithAgg(MAX_SUBQUERIES, "manybigfields", "c = COUNT_DISTINCT(f999)", grouping.toString())
        );
    }

    // It is likely the lack of memory tracking for BlockSourceReader.scratch cause OOM instead of CBE here.
    public void testGiantTextFieldInSubqueryIntermediateResults() throws IOException {
        if (isServerless()) { // 40 docs OOM in serverless
            return;
        }
        // OOM heap dump shows around 45 docs/pages in it locally, which means fetching 45 docs will cause OOM, each page is 5MB.
        // 2 subqueries and 8 subqueries both OOM, however they have different patterns in the heap dump.
        // 1. According to the heap dump of 2 subqueries, the OOM happened during adding page into OutputOperator for returning results,
        // pages and blocks are tracked by CB, BlockSourceReader also show in the heap dump but its scratch is not tracked by CB.
        // 2. According to the heap dump of 8 subqueries, the pattern is very similar as OOM of 8 subqueries in
        // testManyRandomKeywordFieldsInSubqueryIntermediateResults, BlockSourceReader.scratch is not tracked by CB,
        // the constrain is in reading data.
        // TODO improve memory tracking in BlockSourceReader.scratch
        int docs = 40; // 40 docs *5MB does not OOM without subquery, 2 or 8 subqueries OOM
        int limit = 30; // we should not need a limit here, this is temporary to include some coverage on this pattern
        heapAttackIT.initGiantTextField(docs, false, 5);
        for (int subquery : List.of(DEFAULT_SUBQUERIES)) {
            // TODO remove the limit when BlockSourceReader.scratch memory tracking is improved
            Map<?, ?> response = buildSubqueries(subquery, "bigtext", " | limit " + limit);
            // Map<?, ?> response = buildSubqueries(subquery, "bigtext", "");
            ListMatcher columns = matchesList().item(matchesMap().entry("name", "f").entry("type", "text"));
            assertMap(response, matchesMap().entry("columns", columns));
        }
    }

    // It is likely the lack of memory tracking for BlockSourceReader.scratch cause OOM instead of CBE here.
    public void testGiantTextFieldInSubqueryIntermediateResultsWithSort() throws IOException {
        if (isServerless()) { // 40 docs OOM in serverless
            return;
        }
        // Similar observation as no sort case, 2 or 8 subqueries both OOM.
        // TODO improve memory tracking in BlockSourceReader.scratch
        int docs = 40; // 40 docs *5MB does not OOM without subquery
        heapAttackIT.initGiantTextField(docs, false, 5);
        for (int subquery : List.of(DEFAULT_SUBQUERIES)) {
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

    private Map<String, Object> buildSubqueries(int subqueries, String indexName, String limit) throws IOException {
        StringBuilder query = startQuery();
        String subquery = "(FROM " + indexName + " )";
        query.append("FROM ").append(subquery);
        for (int i = 1; i < subqueries; i++) {
            query.append(", ").append(subquery);
        }
        // the limit should not be necessary, it is just to limit the result size for giant text test temporarily
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
        return responseAsMap(query(query.toString(), "columns,values"));
    }
}
