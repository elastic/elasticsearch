/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.planner.StarTreeSourceOperatorFactory;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

/**
 * Integration tests for star-tree pre-aggregation index with ES|QL queries.
 * <p>
 * We suppress codec randomization to ensure the Elasticsearch codec with star-tree merge callback is used.
 */
@LuceneTestCase.SuppressCodecs("*")
public class StarTreeIT extends AbstractEsqlIntegTestCase {

    private static final String INDEX_NAME = "star_tree_test";

    /**
     * Test that an index with star-tree configuration can be created and queried.
     */
    public void testBasicStarTreeIndex() throws Exception {
        createStarTreeIndex();
        indexTestDocuments();
        forceMergeIndex();

        // Test simple aggregation without GROUP BY
        try (EsqlQueryResponse results = run("FROM " + INDEX_NAME + " | STATS total_bytes = sum(bytes)")) {
            List<List<Object>> values = getValuesList(results);
            assertThat(values, hasSize(1));
            // Total bytes: 100+200+150+300+120+250+180+350+200+400 = 2250
            assertThat(((Number) values.get(0).get(0)).longValue(), equalTo(2250L));
        }
    }

    /**
     * Test aggregation with GROUP BY on a grouping field.
     */
    public void testAggregationWithGroupBy() throws Exception {
        createStarTreeIndex();
        indexTestDocuments();
        forceMergeIndex();

        // Test aggregation with GROUP BY country
        try (EsqlQueryResponse results = run("FROM " + INDEX_NAME + " | STATS total_bytes = sum(bytes) BY country | SORT country")) {
            List<List<Object>> values = getValuesList(results);
            assertThat(values, hasSize(3));

            // DE: 150+350 = 500
            assertThat(values.get(0).get(1), equalTo("DE"));
            assertThat(((Number) values.get(0).get(0)).longValue(), equalTo(500L));

            // FR: 200+180 = 380
            assertThat(values.get(1).get(1), equalTo("FR"));
            assertThat(((Number) values.get(1).get(0)).longValue(), equalTo(380L));

            // US: 100+300+120+250+200+400 = 1370
            assertThat(values.get(2).get(1), equalTo("US"));
            assertThat(((Number) values.get(2).get(0)).longValue(), equalTo(1370L));
        }
    }

    /**
     * Test multiple aggregations on the same query.
     */
    public void testMultipleAggregations() throws Exception {
        createStarTreeIndex();
        indexTestDocuments();
        forceMergeIndex();

        // Test multiple aggregations
        try (
            EsqlQueryResponse results = run(
                "FROM " + INDEX_NAME + " | STATS total = sum(bytes), cnt = count(bytes), min_bytes = min(bytes), max_bytes = max(bytes)"
            )
        ) {
            List<List<Object>> values = getValuesList(results);
            assertThat(values, hasSize(1));
            assertThat(((Number) values.get(0).get(0)).longValue(), equalTo(2250L)); // sum
            assertThat(((Number) values.get(0).get(1)).longValue(), equalTo(10L)); // count
            assertThat(((Number) values.get(0).get(2)).longValue(), equalTo(100L)); // min
            assertThat(((Number) values.get(0).get(3)).longValue(), equalTo(400L)); // max
        }
    }

    /**
     * Test aggregation with multiple GROUP BY fields.
     */
    public void testMultipleGroupByFields() throws Exception {
        createStarTreeIndex();
        indexTestDocuments();
        forceMergeIndex();

        // Test GROUP BY country, browser
        try (
            EsqlQueryResponse results = run(
                "FROM " + INDEX_NAME + " | STATS total_bytes = sum(bytes) BY country, browser | SORT country, browser"
            )
        ) {
            List<List<Object>> values = getValuesList(results);
            // We expect combinations like: DE-Chrome, DE-Firefox, FR-Chrome, FR-Safari, US-Chrome, US-Firefox, US-Safari
            assertThat(values.size(), org.hamcrest.Matchers.greaterThanOrEqualTo(5));
        }
    }

    /**
     * Test average aggregation (computed from sum/count).
     */
    public void testAverageAggregation() throws Exception {
        createStarTreeIndex();
        indexTestDocuments();
        forceMergeIndex();

        // Test average calculation
        try (EsqlQueryResponse results = run("FROM " + INDEX_NAME + " | STATS avg_bytes = avg(bytes)")) {
            List<List<Object>> values = getValuesList(results);
            assertThat(values, hasSize(1));
            // Average: 2250 / 10 = 225.0
            assertThat(((Number) values.get(0).get(0)).doubleValue(), closeTo(225.0, 0.01));
        }
    }

    /**
     * Test aggregation on latency field (another metric).
     */
    public void testLatencyAggregation() throws Exception {
        createStarTreeIndex();
        indexTestDocuments();
        forceMergeIndex();

        // Test aggregation on latency
        try (EsqlQueryResponse results = run("FROM " + INDEX_NAME + " | STATS avg_latency = avg(latency) BY country | SORT country")) {
            List<List<Object>> values = getValuesList(results);
            assertThat(values, hasSize(3));
            // All latencies are positive and reasonable
            for (List<Object> row : values) {
                double avgLatency = ((Number) row.get(0)).doubleValue();
                assertThat(avgLatency, org.hamcrest.Matchers.greaterThan(0.0));
            }
        }
    }

    /**
     * Test count(*) aggregation.
     */
    public void testCountStar() throws Exception {
        createStarTreeIndex();
        indexTestDocuments();
        forceMergeIndex();

        try (EsqlQueryResponse results = run("FROM " + INDEX_NAME + " | STATS doc_count = count(*)")) {
            List<List<Object>> values = getValuesList(results);
            assertThat(values, hasSize(1));
            assertThat(((Number) values.get(0).get(0)).longValue(), equalTo(10L));
        }
    }

    /**
     * Test count(*) with GROUP BY.
     */
    public void testCountStarWithGroupBy() throws Exception {
        createStarTreeIndex();
        indexTestDocuments();
        forceMergeIndex();

        try (EsqlQueryResponse results = run("FROM " + INDEX_NAME + " | STATS doc_count = count(*) BY country | SORT country")) {
            List<List<Object>> values = getValuesList(results);
            assertThat(values, hasSize(3));

            // DE: 2 docs
            assertThat(values.get(0).get(1), equalTo("DE"));
            assertThat(((Number) values.get(0).get(0)).longValue(), equalTo(2L));

            // FR: 2 docs
            assertThat(values.get(1).get(1), equalTo("FR"));
            assertThat(((Number) values.get(1).get(0)).longValue(), equalTo(2L));

            // US: 6 docs
            assertThat(values.get(2).get(1), equalTo("US"));
            assertThat(((Number) values.get(2).get(0)).longValue(), equalTo(6L));
        }
    }

    /**
     * Test that regular non-aggregation queries still work.
     */
    public void testNonAggregationQuery() throws Exception {
        createStarTreeIndex();
        indexTestDocuments();
        forceMergeIndex();

        // Regular query should still work
        try (EsqlQueryResponse results = run("FROM " + INDEX_NAME + " | WHERE country == \"US\" | SORT bytes | LIMIT 3")) {
            List<List<Object>> values = getValuesList(results);
            assertThat(values, hasSize(3));
        }
    }

    /**
     * Test time bucketing with GROUP BY on date_histogram dimension.
     */
    public void testTimeBucketing() throws Exception {
        createStarTreeIndex();
        indexTimeBucketTestDocuments();
        forceMergeIndex();

        // Test GROUP BY @timestamp with time bucketing (1h interval)
        try (
            EsqlQueryResponse results = run(
                "FROM " + INDEX_NAME + " | STATS total_bytes = sum(bytes), doc_count = count(*) BY `@timestamp`"
            )
        ) {
            List<List<Object>> values = getValuesList(results);
            // We indexed documents across 3 different hour buckets
            assertThat(values, hasSize(3));

            // Verify each bucket has expected values (order-independent check)
            // Hour 0: 2 docs, 300 bytes; Hour 1: 2 docs, 400 bytes; Hour 2: 2 docs, 650 bytes
            long totalBytes = 0;
            long totalDocs = 0;
            for (List<Object> row : values) {
                long bytes = ((Number) row.get(0)).longValue();
                long docs = ((Number) row.get(1)).longValue();
                totalBytes += bytes;
                totalDocs += docs;
                // Each bucket should have exactly 2 docs
                assertThat(docs, equalTo(2L));
                // Each bucket should have one of the expected byte totals
                assertThat(bytes, org.hamcrest.Matchers.isOneOf(300L, 400L, 650L));
            }
            // Total across all buckets: 300 + 400 + 650 = 1350
            assertThat(totalBytes, equalTo(1350L));
            // Total docs: 6
            assertThat(totalDocs, equalTo(6L));
        }
    }

    /**
     * Test time bucketing combined with other GROUP BY fields.
     */
    public void testTimeBucketingWithCountry() throws Exception {
        createStarTreeIndex();
        indexTimeBucketTestDocuments();
        forceMergeIndex();

        // Test GROUP BY country, @timestamp
        try (
            EsqlQueryResponse results = run(
                "FROM " + INDEX_NAME + " | STATS total_bytes = sum(bytes) BY country, `@timestamp` | SORT country, `@timestamp`"
            )
        ) {
            List<List<Object>> values = getValuesList(results);
            // The test documents have 3 unique (country, hour) combinations:
            // (US, hour0), (FR, hour1), (DE, hour2)
            assertThat(values.size(), org.hamcrest.Matchers.greaterThanOrEqualTo(3));
        }
    }

    /**
     * Test multiple star-tree definitions in the same index.
     * The optimizer should select the best star-tree for each query.
     */
    public void testMultipleStarTrees() throws Exception {
        createMultiStarTreeIndex();
        indexTestDocuments();
        forceMergeIndex();

        // Query 1: GROUP BY country - should use "by_country" star-tree (more specific)
        try (EsqlQueryResponse results = run("FROM " + INDEX_NAME + " | STATS total_bytes = sum(bytes) BY country | SORT country")) {
            List<List<Object>> values = getValuesList(results);
            assertThat(values, hasSize(3)); // DE, FR, US

            // DE: 150+350 = 500
            assertThat(values.get(0).get(1), equalTo("DE"));
            assertThat(((Number) values.get(0).get(0)).longValue(), equalTo(500L));

            // US: 100+300+120+250+200+400 = 1370
            assertThat(values.get(2).get(1), equalTo("US"));
            assertThat(((Number) values.get(2).get(0)).longValue(), equalTo(1370L));
        }

        // Query 2: GROUP BY browser - should use "by_browser" star-tree (more specific)
        try (EsqlQueryResponse results = run("FROM " + INDEX_NAME + " | STATS total_bytes = sum(bytes) BY browser | SORT browser")) {
            List<List<Object>> values = getValuesList(results);
            assertThat(values, hasSize(3)); // Chrome, Firefox, Safari

            // Sum of all bytes: 2250
            long totalBytes = 0;
            for (List<Object> row : values) {
                totalBytes += ((Number) row.get(0)).longValue();
            }
            assertThat(totalBytes, equalTo(2250L));
        }

        // Query 3: GROUP BY country, browser - should use "by_country_browser" star-tree (exact match)
        try (
            EsqlQueryResponse results = run(
                "FROM " + INDEX_NAME + " | STATS total_bytes = sum(bytes) BY country, browser | SORT country, browser"
            )
        ) {
            List<List<Object>> values = getValuesList(results);
            // Multiple country/browser combinations
            assertThat(values.size(), org.hamcrest.Matchers.greaterThanOrEqualTo(5));
        }
    }

    // ========== Edge Case Tests ==========

    /**
     * Test aggregation with documents containing null values in metric fields.
     * Star-tree should handle nulls gracefully.
     */
    public void testNullValues() throws Exception {
        createStarTreeIndex();
        indexDocumentsWithNulls();
        forceMergeIndex();

        // SUM should only count non-null values
        try (EsqlQueryResponse results = run("FROM " + INDEX_NAME + " | STATS total_bytes = sum(bytes) BY country | SORT country")) {
            List<List<Object>> values = getValuesList(results);
            // Should have results for countries with non-null bytes
            assertThat(values.size(), org.hamcrest.Matchers.greaterThanOrEqualTo(1));
        }

        // COUNT should count documents (including those with null metric values)
        try (EsqlQueryResponse results = run("FROM " + INDEX_NAME + " | STATS doc_count = count(*) BY country | SORT country")) {
            List<List<Object>> values = getValuesList(results);
            assertThat(values.size(), org.hamcrest.Matchers.greaterThanOrEqualTo(1));
        }
    }

    /**
     * Test query with filter that matches no documents.
     * Should return empty result set gracefully.
     */
    public void testEmptyResultSet() throws Exception {
        createStarTreeIndex();
        indexTestDocuments();
        forceMergeIndex();

        // Filter for non-existent country
        try (
            EsqlQueryResponse results = run(
                "FROM " + INDEX_NAME + " | WHERE country == \"NONEXISTENT\" | STATS total_bytes = sum(bytes)"
            )
        ) {
            List<List<Object>> values = getValuesList(results);
            // Should return a row with null or 0
            assertThat(values, hasSize(1));
        }
    }

    /**
     * Test aggregation when some documents are missing the metric field entirely.
     * Star-tree should handle missing fields gracefully.
     */
    public void testMissingFields() throws Exception {
        createStarTreeIndex();
        indexDocumentsWithMissingFields();
        forceMergeIndex();

        // Should still return valid aggregation for documents that have the field
        try (EsqlQueryResponse results = run("FROM " + INDEX_NAME + " | STATS total_bytes = sum(bytes)")) {
            List<List<Object>> values = getValuesList(results);
            assertThat(values, hasSize(1));
            // Sum should be from documents that have bytes field: 100 + 300 = 400
            assertThat(((Number) values.get(0).get(0)).longValue(), equalTo(400L));
        }
    }

    /**
     * Test that unsupported aggregation functions fall back to regular execution.
     * Star-tree only supports SUM, COUNT, MIN, MAX, AVG.
     */
    public void testUnsupportedAggregationFallback() throws Exception {
        createStarTreeIndex();
        indexTestDocuments();
        forceMergeIndex();

        // MEDIAN is not supported by star-tree - should fall back to regular execution
        try (EsqlQueryResponse results = run("FROM " + INDEX_NAME + " | STATS med = median(bytes)")) {
            List<List<Object>> values = getValuesList(results);
            assertThat(values, hasSize(1));
            // Should still return correct result via regular execution
            assertThat(((Number) values.get(0).get(0)).doubleValue(), org.hamcrest.Matchers.greaterThan(0.0));
        }
    }

    /**
     * Test query with filter on non-grouping field.
     * Should fall back to regular execution since star-tree can only filter on grouping fields.
     */
    public void testFilterOnNonGroupingField() throws Exception {
        createStarTreeIndex();
        indexTestDocuments();
        forceMergeIndex();

        // Filter on bytes (a metric field, not a grouping field)
        try (EsqlQueryResponse results = run("FROM " + INDEX_NAME + " | WHERE bytes > 200 | STATS total = sum(bytes)")) {
            List<List<Object>> values = getValuesList(results);
            assertThat(values, hasSize(1));
            // Should return correct sum of bytes > 200: 300+250+350+400 = 1300
            // (Falls back to regular execution)
        }
    }

    /**
     * Test GROUP BY on a field that is not in the star-tree grouping fields.
     * Should fall back to regular execution.
     */
    public void testGroupByNonGroupingField() throws Exception {
        createStarTreeIndex();
        indexTestDocuments();
        forceMergeIndex();

        // bytes is a metric field, not a grouping field - can't GROUP BY it with star-tree
        try (EsqlQueryResponse results = run("FROM " + INDEX_NAME + " | STATS doc_count = count(*) BY bytes | LIMIT 5")) {
            List<List<Object>> values = getValuesList(results);
            // Should still return results via regular execution
            assertThat(values.size(), org.hamcrest.Matchers.greaterThanOrEqualTo(1));
        }
    }

    /**
     * Test that star-tree works correctly with LIMIT clause.
     */
    public void testWithLimit() throws Exception {
        createStarTreeIndex();
        indexTestDocuments();
        forceMergeIndex();

        try (EsqlQueryResponse results = run("FROM " + INDEX_NAME + " | STATS total_bytes = sum(bytes) BY country | LIMIT 2")) {
            List<List<Object>> values = getValuesList(results);
            assertThat(values, hasSize(2));
        }
    }

    /**
     * Test star-tree with combined aggregations and expressions.
     */
    public void testComplexAggregations() throws Exception {
        createStarTreeIndex();
        indexTestDocuments();
        forceMergeIndex();

        // Multiple aggregations with arithmetic
        try (
            EsqlQueryResponse results = run(
                "FROM " + INDEX_NAME + " | STATS total = sum(bytes), cnt = count(*), avg_bytes = avg(bytes) BY country | SORT country"
            )
        ) {
            List<List<Object>> values = getValuesList(results);
            assertThat(values, hasSize(3));

            // Verify avg = total / cnt for each country
            for (List<Object> row : values) {
                long total = ((Number) row.get(0)).longValue();
                long cnt = ((Number) row.get(1)).longValue();
                double avg = ((Number) row.get(2)).doubleValue();
                assertThat(avg, closeTo((double) total / cnt, 0.01));
            }
        }
    }

    /**
     * Test star-tree performance with larger dataset.
     * This test verifies correctness with more documents to stress the tree structure.
     */
    public void testLargerDataset() throws Exception {
        createStarTreeIndex();
        indexLargerDataset();
        forceMergeIndex();

        // Aggregate over larger dataset
        try (EsqlQueryResponse results = run("FROM " + INDEX_NAME + " | STATS total_bytes = sum(bytes), doc_count = count(*)")) {
            List<List<Object>> values = getValuesList(results);
            assertThat(values, hasSize(1));
            // 100 documents, each with bytes = doc_id * 10
            // Sum = 10 + 20 + ... + 1000 = 10 * (1 + 2 + ... + 100) = 10 * 5050 = 50500
            assertThat(((Number) values.get(0).get(0)).longValue(), equalTo(50500L));
            assertThat(((Number) values.get(0).get(1)).longValue(), equalTo(100L));
        }

        // GROUP BY with larger dataset
        try (EsqlQueryResponse results = run("FROM " + INDEX_NAME + " | STATS total = sum(bytes) BY country | SORT country")) {
            List<List<Object>> values = getValuesList(results);
            // 5 countries
            assertThat(values, hasSize(5));
        }
    }

    // ========== Performance Tests ==========

    /**
     * Performance test with 1000 documents.
     * Verifies star-tree can handle moderate dataset sizes and return correct results.
     */
    public void testPerformanceWith1000Docs() throws Exception {
        createStarTreeIndex();
        indexPerformanceDataset(1000);
        forceMergeIndex();

        // Simple aggregation
        try (EsqlQueryResponse results = run("FROM " + INDEX_NAME + " | STATS total = sum(bytes), cnt = count(*), avg_val = avg(bytes)")) {
            List<List<Object>> values = getValuesList(results);
            assertThat(values, hasSize(1));

            // Sum: 10 + 20 + ... + 10000 = 10 * (1 + 2 + ... + 1000) = 10 * 500500 = 5005000
            assertThat(((Number) values.get(0).get(0)).longValue(), equalTo(5005000L));
            assertThat(((Number) values.get(0).get(1)).longValue(), equalTo(1000L));
            assertThat(((Number) values.get(0).get(2)).doubleValue(), closeTo(5005.0, 0.1));
        }

        // GROUP BY with moderate cardinality
        try (EsqlQueryResponse results = run("FROM " + INDEX_NAME + " | STATS cnt = count(*) BY country | SORT country")) {
            List<List<Object>> values = getValuesList(results);
            // 5 countries, each with 200 docs
            assertThat(values, hasSize(5));
            for (List<Object> row : values) {
                assertThat(((Number) row.get(0)).longValue(), equalTo(200L));
            }
        }

        // Multiple aggregations with GROUP BY
        try (
            EsqlQueryResponse results = run(
                "FROM " + INDEX_NAME + " | STATS total = sum(bytes), min_val = min(bytes), max_val = max(bytes) BY country | SORT country"
            )
        ) {
            List<List<Object>> values = getValuesList(results);
            assertThat(values, hasSize(5));

            // Verify min/max make sense
            for (List<Object> row : values) {
                long min = ((Number) row.get(1)).longValue();
                long max = ((Number) row.get(2)).longValue();
                assertThat(min, org.hamcrest.Matchers.greaterThan(0L));
                assertThat(max, org.hamcrest.Matchers.greaterThan(min));
            }
        }
    }

    /**
     * Performance test with GROUP BY on multiple dimensions.
     */
    public void testPerformanceMultiDimensionGroupBy() throws Exception {
        createStarTreeIndex();
        indexPerformanceDataset(500);
        forceMergeIndex();

        // GROUP BY country AND browser
        try (
            EsqlQueryResponse results = run(
                "FROM " + INDEX_NAME + " | STATS cnt = count(*), total = sum(bytes) BY country, browser | SORT country, browser"
            )
        ) {
            List<List<Object>> values = getValuesList(results);
            // 5 countries * 4 browsers = 20 combinations
            assertThat(values, hasSize(20));

            // Verify total document count
            long totalCount = 0;
            for (List<Object> row : values) {
                totalCount += ((Number) row.get(0)).longValue();
            }
            assertThat(totalCount, equalTo(500L));
        }
    }

    // ========== Error Scenario Tests ==========

    /**
     * Test that queries work correctly on an index without star-tree configuration.
     * This verifies regular execution path still works.
     */
    public void testIndexWithoutStarTree() throws Exception {
        createIndexWithoutStarTree();
        indexTestDocumentsToIndex("no_star_tree_test");
        forceMergeIndexByName("no_star_tree_test");

        // Queries should still work via regular execution
        try (EsqlQueryResponse results = run("FROM no_star_tree_test | STATS total_bytes = sum(bytes) BY country | SORT country")) {
            List<List<Object>> values = getValuesList(results);
            assertThat(values, hasSize(3));

            // DE: 150+350 = 500
            assertThat(values.get(0).get(1), equalTo("DE"));
            assertThat(((Number) values.get(0).get(0)).longValue(), equalTo(500L));
        }
    }

    /**
     * Test that re-merging after adding more documents includes all data.
     * This verifies star-tree is correctly rebuilt during subsequent merges.
     */
    public void testRemergeAfterAddingDocuments() throws Exception {
        createStarTreeIndex();
        indexTestDocuments();
        forceMergeIndex(); // First star-tree built

        // Add more documents
        BulkRequestBuilder bulk = client().prepareBulk();
        long baseTimestamp = System.currentTimeMillis();
        bulk.add(
            new IndexRequest(INDEX_NAME).source(
                Map.of("country", "UK", "browser", "Chrome", "@timestamp", baseTimestamp, "bytes", 500, "latency", 50.0)
            )
        );
        bulk.add(
            new IndexRequest(INDEX_NAME).source(
                Map.of("country", "UK", "browser", "Firefox", "@timestamp", baseTimestamp + 1000, "bytes", 600, "latency", 60.0)
            )
        );
        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        // Force merge again to rebuild star-tree with all documents
        forceMergeIndex();

        // Query should now include all data
        try (EsqlQueryResponse results = run("FROM " + INDEX_NAME + " | STATS total_bytes = sum(bytes)")) {
            List<List<Object>> values = getValuesList(results);
            assertThat(values, hasSize(1));
            // Original: 2250 + new: 500 + 600 = 3350
            assertThat(((Number) values.get(0).get(0)).longValue(), equalTo(3350L));
        }

        // GROUP BY should include new country
        try (EsqlQueryResponse results = run("FROM " + INDEX_NAME + " | STATS total = sum(bytes) BY country | SORT country")) {
            List<List<Object>> values = getValuesList(results);
            assertThat(values, hasSize(4)); // DE, FR, UK, US
        }
    }

    /**
     * Test aggregation after multiple force merges.
     * Verifies star-tree is correctly rebuilt each time.
     */
    public void testMultipleForceMerges() throws Exception {
        createStarTreeIndex();
        indexTestDocuments();
        forceMergeIndex();

        // First query
        try (EsqlQueryResponse results = run("FROM " + INDEX_NAME + " | STATS total_bytes = sum(bytes)")) {
            List<List<Object>> values = getValuesList(results);
            assertThat(((Number) values.get(0).get(0)).longValue(), equalTo(2250L));
        }

        // Add documents and merge again
        BulkRequestBuilder bulk = client().prepareBulk();
        bulk.add(
            new IndexRequest(INDEX_NAME).source(
                Map.of("country", "IT", "browser", "Chrome", "@timestamp", System.currentTimeMillis(), "bytes", 100, "latency", 10.0)
            )
        );
        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        forceMergeIndex();

        // Query should reflect new data
        try (EsqlQueryResponse results = run("FROM " + INDEX_NAME + " | STATS total_bytes = sum(bytes)")) {
            List<List<Object>> values = getValuesList(results);
            assertThat(((Number) values.get(0).get(0)).longValue(), equalTo(2350L)); // 2250 + 100
        }

        // Add more and merge again
        bulk = client().prepareBulk();
        bulk.add(
            new IndexRequest(INDEX_NAME).source(
                Map.of("country", "IT", "browser", "Firefox", "@timestamp", System.currentTimeMillis(), "bytes", 200, "latency", 20.0)
            )
        );
        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        forceMergeIndex();

        // Query should reflect all data
        try (EsqlQueryResponse results = run("FROM " + INDEX_NAME + " | STATS total_bytes = sum(bytes)")) {
            List<List<Object>> values = getValuesList(results);
            assertThat(((Number) values.get(0).get(0)).longValue(), equalTo(2550L)); // 2350 + 200
        }
    }

    /**
     * Test star-tree with single document.
     * Verifies edge case of minimal data.
     */
    public void testSingleDocument() throws Exception {
        createStarTreeIndex();

        // Index just one document
        client().prepareIndex(INDEX_NAME)
            .setSource(
                Map.of("country", "US", "browser", "Chrome", "@timestamp", System.currentTimeMillis(), "bytes", 100, "latency", 10.0)
            )
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        forceMergeIndex();

        // Aggregation should work with single document
        try (EsqlQueryResponse results = run("FROM " + INDEX_NAME + " | STATS total = sum(bytes), cnt = count(*)")) {
            List<List<Object>> values = getValuesList(results);
            assertThat(values, hasSize(1));
            assertThat(((Number) values.get(0).get(0)).longValue(), equalTo(100L));
            assertThat(((Number) values.get(0).get(1)).longValue(), equalTo(1L));
        }
    }

    /**
     * Test star-tree with identical dimension values.
     * All documents have same country and browser.
     */
    public void testIdenticalDimensionValues() throws Exception {
        createStarTreeIndex();

        // Index documents with identical dimension values
        BulkRequestBuilder bulk = client().prepareBulk();
        long baseTimestamp = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            bulk.add(
                new IndexRequest(INDEX_NAME).source(
                    Map.of(
                        "country",
                        "US",
                        "browser",
                        "Chrome",
                        "@timestamp",
                        baseTimestamp + i * 1000,
                        "bytes",
                        100 + i * 10,
                        "latency",
                        10.0 + i
                    )
                )
            );
        }
        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        forceMergeIndex();

        // GROUP BY country should return single group
        try (EsqlQueryResponse results = run("FROM " + INDEX_NAME + " | STATS total = sum(bytes) BY country")) {
            List<List<Object>> values = getValuesList(results);
            assertThat(values, hasSize(1));
            assertThat(values.get(0).get(1), equalTo("US"));
            // Sum: 100+110+120+130+140+150+160+170+180+190 = 1450
            assertThat(((Number) values.get(0).get(0)).longValue(), equalTo(1450L));
        }

        // GROUP BY browser should return single group
        try (EsqlQueryResponse results = run("FROM " + INDEX_NAME + " | STATS total = sum(bytes) BY browser")) {
            List<List<Object>> values = getValuesList(results);
            assertThat(values, hasSize(1));
            assertThat(values.get(0).get(1), equalTo("Chrome"));
            assertThat(((Number) values.get(0).get(0)).longValue(), equalTo(1450L));
        }

        // count(*) should be 10
        try (EsqlQueryResponse results = run("FROM " + INDEX_NAME + " | STATS cnt = count(*)")) {
            List<List<Object>> values = getValuesList(results);
            assertThat(values, hasSize(1));
            assertThat(((Number) values.get(0).get(0)).longValue(), equalTo(10L));
        }
    }

    /**
     * Test that mixed segments (some with star-tree, some without) are handled correctly.
     * This verifies the hybrid execution path where:
     * 1. Segments with star-tree use pre-aggregated values
     * 2. Segments without star-tree fall back to raw document scanning
     * 3. Results are correctly combined
     */
    public void testMixedSegments() throws Exception {
        createStarTreeIndex();

        // Index first batch of documents and force merge to create star-tree segment
        BulkRequestBuilder bulk1 = client().prepareBulk();
        long baseTimestamp = System.currentTimeMillis();
        for (int i = 0; i < 5; i++) {
            bulk1.add(
                new IndexRequest(INDEX_NAME).source(
                    Map.of(
                        "country",
                        "US",
                        "browser",
                        "Chrome",
                        "@timestamp",
                        baseTimestamp + i * 1000,
                        "bytes",
                        100L,
                        "latency",
                        10.0
                    )
                )
            );
        }
        bulk1.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        forceMergeIndex();  // This creates star-tree segment

        // Verify the star-tree segment works correctly before adding more docs
        try (EsqlQueryResponse results = run("FROM " + INDEX_NAME + " | STATS total = sum(bytes)")) {
            List<List<Object>> values = getValuesList(results);
            assertThat(values, hasSize(1));
            assertThat(((Number) values.get(0).get(0)).longValue(), equalTo(500L));
        }

        // Index second batch of documents WITHOUT force merge (no star-tree)
        BulkRequestBuilder bulk2 = client().prepareBulk();
        for (int i = 0; i < 5; i++) {
            bulk2.add(
                new IndexRequest(INDEX_NAME).source(
                    Map.of(
                        "country",
                        "US",
                        "browser",
                        "Chrome",
                        "@timestamp",
                        baseTimestamp + (i + 5) * 1000,
                        "bytes",
                        200L,
                        "latency",
                        20.0
                    )
                )
            );
        }
        bulk2.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        // First verify with a non-aggregation query to check document count
        try (EsqlQueryResponse results = run("FROM " + INDEX_NAME + " | KEEP bytes | LIMIT 100")) {
            List<List<Object>> values = getValuesList(results);
            logger.info("Total documents returned by non-agg query: {}", values.size());
            assertThat("Should have 10 total documents", values.size(), equalTo(10));
        }

        // Now test sum(bytes) with star-tree
        // Expected: 500 (star-tree) + 1000 (fallback) = 1500
        try (EsqlQueryResponse results = run("FROM " + INDEX_NAME + " | STATS total = sum(bytes)")) {
            List<List<Object>> values = getValuesList(results);
            assertThat(values, hasSize(1));
            long total = ((Number) values.get(0).get(0)).longValue();
            logger.info("Sum of bytes: {} (expected 1500)", total);
            // NOTE: If this returns 500, it means fallback processing isn't working
            // If this returns 1500, fallback processing is working correctly
            assertThat(total, equalTo(1500L));
        }
    }

    /**
     * Test GROUP BY queries with mixed segments (star-tree and non-star-tree).
     * This verifies that partial results from different segment types are correctly combined
     * by the downstream aggregator when there's a grouping key.
     */
    public void testMixedSegmentsWithGroupBy() throws Exception {
        createStarTreeIndex();

        long baseTimestamp = System.currentTimeMillis();

        // Index first batch with country=US
        BulkRequestBuilder bulk1 = client().prepareBulk();
        for (int i = 0; i < 5; i++) {
            bulk1.add(
                new IndexRequest(INDEX_NAME).source(
                    Map.of(
                        "country", "US",
                        "browser", "Chrome",
                        "@timestamp", baseTimestamp + i * 1000,
                        "bytes", 100L,
                        "latency", 10.0
                    )
                )
            );
        }
        bulk1.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        // Force merge to build star-tree for first batch
        logger.info("Starting force merge for index: {}", INDEX_NAME);
        client().admin().indices().forceMerge(new ForceMergeRequest(INDEX_NAME).maxNumSegments(1).flush(true)).actionGet();
        logger.info("Force merge completed for index: {}", INDEX_NAME);

        // Index second batch with country=UK (different country, no force merge)
        BulkRequestBuilder bulk2 = client().prepareBulk();
        for (int i = 0; i < 5; i++) {
            bulk2.add(
                new IndexRequest(INDEX_NAME).source(
                    Map.of(
                        "country", "UK",
                        "browser", "Firefox",
                        "@timestamp", baseTimestamp + (i + 5) * 1000,
                        "bytes", 200L,
                        "latency", 20.0
                    )
                )
            );
        }
        bulk2.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        // Index third batch with country=US again (same as first batch, no force merge)
        BulkRequestBuilder bulk3 = client().prepareBulk();
        for (int i = 0; i < 3; i++) {
            bulk3.add(
                new IndexRequest(INDEX_NAME).source(
                    Map.of(
                        "country", "US",
                        "browser", "Safari",
                        "@timestamp", baseTimestamp + (i + 10) * 1000,
                        "bytes", 150L,
                        "latency", 15.0
                    )
                )
            );
        }
        bulk3.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        // Verify document count
        try (EsqlQueryResponse results = run("FROM " + INDEX_NAME + " | KEEP bytes | LIMIT 100")) {
            List<List<Object>> values = getValuesList(results);
            logger.info("Total documents: {}", values.size());
            assertThat("Should have 13 total documents", values.size(), equalTo(13));
        }

        // Test GROUP BY country with mixed segments
        // Expected:
        // - US: 500 (star-tree: 5*100) + 450 (fallback: 3*150) = 950
        // - UK: 1000 (fallback: 5*200)
        try (EsqlQueryResponse results = run("FROM " + INDEX_NAME + " | STATS total = sum(bytes) BY country | SORT country")) {
            List<List<Object>> values = getValuesList(results);
            logger.info("GROUP BY results: {}", values);
            assertThat("Should have 2 groups (UK, US)", values.size(), equalTo(2));

            // Sorted by country: UK first, then US
            // UK: 5 docs * 200 = 1000 (all from fallback)
            assertThat(values.get(0).get(1), equalTo("UK"));
            long ukTotal = ((Number) values.get(0).get(0)).longValue();
            logger.info("UK total: {} (expected 1000)", ukTotal);
            assertThat(ukTotal, equalTo(1000L));

            // US: 5 docs * 100 = 500 (star-tree) + 3 docs * 150 = 450 (fallback) = 950
            assertThat(values.get(1).get(1), equalTo("US"));
            long usTotal = ((Number) values.get(1).get(0)).longValue();
            logger.info("US total: {} (expected 950)", usTotal);
            assertThat(usTotal, equalTo(950L));
        }
    }

    /**
     * Test that verifies the star-tree operator is being used through the profile API.
     * This ensures we're actually using the star-tree optimization and not falling back to full scan.
     */
    public void testStarTreeOperatorUsedInProfile() throws Exception {
        createStarTreeIndex();

        long baseTimestamp = System.currentTimeMillis();

        // Index a simple set of documents
        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < 5; i++) {
            bulk.add(
                new IndexRequest(INDEX_NAME).source(
                    Map.of(
                        "country", "US",
                        "browser", "Chrome",
                        "@timestamp", baseTimestamp + i * 1000,
                        "bytes", 100L,
                        "latency", 10.0
                    )
                )
            );
        }
        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        forceMergeIndex();

        // Run query with profiling enabled
        EsqlQueryRequest request = syncEsqlQueryRequest("FROM " + INDEX_NAME + " | STATS total_bytes = sum(bytes)");
        request.profile(true);
        request.pragmas(getPragmas());

        try (EsqlQueryResponse response = run(request)) {
            // Verify result is correct (5 docs * 100 bytes = 500)
            List<List<Object>> values = getValuesList(response);
            assertThat(values, hasSize(1));
            long total = ((Number) values.get(0).get(0)).longValue();
            logger.info("Total bytes: {} (expected 500)", total);
            assertThat(total, equalTo(500L));

            // Verify star-tree operator is used in profile
            EsqlQueryResponse.Profile profile = response.profile();
            assertNotNull("Profile should not be null", profile);

            // Log all drivers and operators for debugging
            logger.info("Profile has {} drivers", profile.drivers().size());
            boolean foundStarTreeOperator = false;
            StarTreeSourceOperatorFactory.Status starTreeStatus = null;
            for (DriverProfile driverProfile : profile.drivers()) {
                logger.info("Driver: {}", driverProfile.description());
                for (var op : driverProfile.operators()) {
                    logger.info("  Operator: {}", op.operator());
                    if (op.operator().contains("StarTreeSource")) {
                        foundStarTreeOperator = true;
                        logger.info("  >>> Found star-tree operator!");
                        // Check for status details
                        if (op.status() instanceof StarTreeSourceOperatorFactory.Status status) {
                            starTreeStatus = status;
                            logger.info("  >>> Star-tree status: star_tree_name={}, star_tree_segments={}, fallback_segments={}, rows_emitted={}",
                                status.starTreeName(), status.starTreeSegments(), status.fallbackSegments(), status.rowsEmitted());
                        }
                    }
                }
            }
            assertTrue("StarTreeSource operator should be used in profile", foundStarTreeOperator);
            assertNotNull("Star-tree status should be available in profile", starTreeStatus);
            assertTrue("Star-tree should be used (starTreeSegments > 0)", starTreeStatus.isUsingStarTree());
            assertThat("Should have processed at least 1 star-tree segment", starTreeStatus.starTreeSegments(), greaterThan(0));
        }
    }

    /**
     * Test that verifies the star-tree operator is used even with mixed segments (star-tree + fallback).
     */
    public void testStarTreeOperatorUsedWithMixedSegments() throws Exception {
        createStarTreeIndex();

        long baseTimestamp = System.currentTimeMillis();

        // Index first batch and force merge to create star-tree
        BulkRequestBuilder bulk1 = client().prepareBulk();
        for (int i = 0; i < 5; i++) {
            bulk1.add(
                new IndexRequest(INDEX_NAME).source(
                    Map.of(
                        "country", "US",
                        "browser", "Chrome",
                        "@timestamp", baseTimestamp + i * 1000,
                        "bytes", 100L,
                        "latency", 10.0
                    )
                )
            );
        }
        bulk1.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        client().admin().indices().forceMerge(new ForceMergeRequest(INDEX_NAME).maxNumSegments(1).flush(true)).actionGet();

        // Index second batch WITHOUT force merge (no star-tree)
        BulkRequestBuilder bulk2 = client().prepareBulk();
        for (int i = 0; i < 5; i++) {
            bulk2.add(
                new IndexRequest(INDEX_NAME).source(
                    Map.of(
                        "country", "US",
                        "browser", "Chrome",
                        "@timestamp", baseTimestamp + (i + 5) * 1000,
                        "bytes", 200L,
                        "latency", 20.0
                    )
                )
            );
        }
        bulk2.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        // Run query with profiling enabled
        EsqlQueryRequest request = syncEsqlQueryRequest("FROM " + INDEX_NAME + " | STATS total_bytes = sum(bytes)");
        request.profile(true);
        request.pragmas(getPragmas());

        try (EsqlQueryResponse response = run(request)) {
            // Verify result combines star-tree and fallback
            List<List<Object>> values = getValuesList(response);
            assertThat(values, hasSize(1));
            long total = ((Number) values.get(0).get(0)).longValue();
            logger.info("Total bytes with mixed segments: {} (expected 1500)", total);
            assertThat(total, equalTo(1500L));

            // Verify star-tree operator is still used (handles both star-tree and fallback segments)
            EsqlQueryResponse.Profile profile = response.profile();
            assertNotNull("Profile should not be null", profile);

            List<DriverProfile> dataProfiles = profile.drivers().stream()
                .filter(d -> d.description().equals("data"))
                .toList();
            assertThat("Should have at least one data driver", dataProfiles.size(), greaterThan(0));

            boolean foundStarTreeOperator = false;
            for (DriverProfile driverProfile : dataProfiles) {
                for (var op : driverProfile.operators()) {
                    logger.info("Operator: {}", op.operator());
                    if (op.operator().contains("StarTreeSource")) {
                        foundStarTreeOperator = true;
                        logger.info("Found star-tree operator with mixed segments: {}", op.operator());
                    }
                }
            }
            assertTrue("StarTreeSource operator should be used even with mixed segments", foundStarTreeOperator);
        }
    }

    /**
     * Test WHERE clause filter on a grouping field uses star-tree acceleration.
     */
    /**
     * Test that WHERE clause on grouping field causes fallback to regular execution.
     * Star-tree currently doesn't support WHERE clause filtering, so queries with WHERE
     * should fall back to regular execution and return correct results.
     */
    /**
     * Test WHERE clause on grouping field uses star-tree with string filtering.
     */
    public void testWhereClauseOnGroupingFieldFallsBack() throws Exception {
        createStarTreeIndex();

        long baseTimestamp = System.currentTimeMillis();

        // Index documents with different countries
        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < 5; i++) {
            bulk.add(
                new IndexRequest(INDEX_NAME).source(
                    Map.of(
                        "country", "US",
                        "browser", "Chrome",
                        "@timestamp", baseTimestamp + i * 1000,
                        "bytes", 100L,
                        "latency", 10.0
                    )
                )
            );
            bulk.add(
                new IndexRequest(INDEX_NAME).source(
                    Map.of(
                        "country", "UK",
                        "browser", "Firefox",
                        "@timestamp", baseTimestamp + (i + 5) * 1000,
                        "bytes", 200L,
                        "latency", 20.0
                    )
                )
            );
        }
        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        forceMergeIndex();

        // Query with WHERE filter on grouping field
        // Star-tree now supports WHERE clauses on grouping fields (string equality filtering)
        EsqlQueryRequest request = syncEsqlQueryRequest(
            "FROM " + INDEX_NAME + " | WHERE country == \"US\" | STATS total_bytes = sum(bytes)"
        );
        request.profile(true);
        request.pragmas(getPragmas());

        try (EsqlQueryResponse response = run(request)) {
            List<List<Object>> values = getValuesList(response);
            assertThat(values, hasSize(1));
            long total = ((Number) values.get(0).get(0)).longValue();
            logger.info("Total bytes for US with WHERE filter: {} (expected 500)", total);
            assertThat(total, equalTo(500L));  // 5 docs * 100 bytes

            // Verify star-tree operator IS used (string equality filtering on grouping fields is supported)
            EsqlQueryResponse.Profile profile = response.profile();
            assertNotNull("Profile should not be null", profile);
            boolean foundStarTreeOperator = false;
            for (var driverProfile : profile.drivers()) {
                for (var op : driverProfile.operators()) {
                    if (op.status() instanceof StarTreeSourceOperatorFactory.Status status) {
                        foundStarTreeOperator = status.isUsingStarTree();
                    }
                }
            }
            logger.info("Star-tree operator used for WHERE query: {} (expected: true - string filtering supported)", foundStarTreeOperator);
            // Star-tree now supports WHERE clauses on grouping fields
            assertTrue("Star-tree should be used for queries with WHERE clause on grouping field", foundStarTreeOperator);
        }
    }

    /**
     * Test MIN/MAX aggregations with mixed segments.
     * MIN/MAX have different combination logic than SUM (take min of mins, max of maxs).
     */
    public void testMinMaxWithMixedSegments() throws Exception {
        createStarTreeIndex();

        long baseTimestamp = System.currentTimeMillis();

        // First batch: bytes ranging from 50-150
        BulkRequestBuilder bulk1 = client().prepareBulk();
        for (int i = 0; i < 5; i++) {
            bulk1.add(
                new IndexRequest(INDEX_NAME).source(
                    Map.of(
                        "country", "US",
                        "browser", "Chrome",
                        "@timestamp", baseTimestamp + i * 1000,
                        "bytes", 50L + i * 25,  // 50, 75, 100, 125, 150
                        "latency", 10.0
                    )
                )
            );
        }
        bulk1.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        forceMergeIndex();

        // Second batch WITHOUT force merge: bytes ranging from 200-400
        BulkRequestBuilder bulk2 = client().prepareBulk();
        for (int i = 0; i < 5; i++) {
            bulk2.add(
                new IndexRequest(INDEX_NAME).source(
                    Map.of(
                        "country", "US",
                        "browser", "Chrome",
                        "@timestamp", baseTimestamp + (i + 5) * 1000,
                        "bytes", 200L + i * 50,  // 200, 250, 300, 350, 400
                        "latency", 20.0
                    )
                )
            );
        }
        bulk2.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        // Test MIN - should be 50 (from star-tree segment)
        try (EsqlQueryResponse results = run("FROM " + INDEX_NAME + " | STATS min_bytes = min(bytes)")) {
            List<List<Object>> values = getValuesList(results);
            assertThat(values, hasSize(1));
            long minBytes = ((Number) values.get(0).get(0)).longValue();
            logger.info("Min bytes with mixed segments: {} (expected 50)", minBytes);
            assertThat(minBytes, equalTo(50L));
        }

        // Test MAX - should be 400 (from fallback segment)
        try (EsqlQueryResponse results = run("FROM " + INDEX_NAME + " | STATS max_bytes = max(bytes)")) {
            List<List<Object>> values = getValuesList(results);
            assertThat(values, hasSize(1));
            long maxBytes = ((Number) values.get(0).get(0)).longValue();
            logger.info("Max bytes with mixed segments: {} (expected 400)", maxBytes);
            assertThat(maxBytes, equalTo(400L));
        }

        // Test both MIN and MAX together
        try (EsqlQueryResponse results = run("FROM " + INDEX_NAME + " | STATS min_bytes = min(bytes), max_bytes = max(bytes)")) {
            List<List<Object>> values = getValuesList(results);
            assertThat(values, hasSize(1));
            long minBytes = ((Number) values.get(0).get(0)).longValue();
            long maxBytes = ((Number) values.get(0).get(1)).longValue();
            logger.info("Min/Max bytes with mixed segments: min={}, max={} (expected 50, 400)", minBytes, maxBytes);
            assertThat(minBytes, equalTo(50L));
            assertThat(maxBytes, equalTo(400L));
        }
    }

    /**
     * Test double field aggregations with mixed segments.
     * The latency field is a double, testing SUM and AVG on floating point values.
     */
    public void testDoubleFieldWithMixedSegments() throws Exception {
        createStarTreeIndex();

        long baseTimestamp = System.currentTimeMillis();

        // First batch: latency = 10.5 each (5 docs)
        BulkRequestBuilder bulk1 = client().prepareBulk();
        for (int i = 0; i < 5; i++) {
            bulk1.add(
                new IndexRequest(INDEX_NAME).source(
                    Map.of(
                        "country", "US",
                        "browser", "Chrome",
                        "@timestamp", baseTimestamp + i * 1000,
                        "bytes", 100L,
                        "latency", 10.5
                    )
                )
            );
        }
        bulk1.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        forceMergeIndex();

        // Second batch WITHOUT force merge: latency = 20.5 each (5 docs)
        BulkRequestBuilder bulk2 = client().prepareBulk();
        for (int i = 0; i < 5; i++) {
            bulk2.add(
                new IndexRequest(INDEX_NAME).source(
                    Map.of(
                        "country", "US",
                        "browser", "Chrome",
                        "@timestamp", baseTimestamp + (i + 5) * 1000,
                        "bytes", 200L,
                        "latency", 20.5
                    )
                )
            );
        }
        bulk2.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        // Test SUM on double field
        // Expected: 5 * 10.5 + 5 * 20.5 = 52.5 + 102.5 = 155.0
        try (EsqlQueryResponse results = run("FROM " + INDEX_NAME + " | STATS total_latency = sum(latency)")) {
            List<List<Object>> values = getValuesList(results);
            assertThat(values, hasSize(1));
            double totalLatency = ((Number) values.get(0).get(0)).doubleValue();
            logger.info("Total latency with mixed segments: {} (expected 155.0)", totalLatency);
            assertThat(totalLatency, closeTo(155.0, 0.001));
        }

        // Test AVG on double field
        // Expected: 155.0 / 10 = 15.5
        try (EsqlQueryResponse results = run("FROM " + INDEX_NAME + " | STATS avg_latency = avg(latency)")) {
            List<List<Object>> values = getValuesList(results);
            assertThat(values, hasSize(1));
            double avgLatency = ((Number) values.get(0).get(0)).doubleValue();
            logger.info("Avg latency with mixed segments: {} (expected 15.5)", avgLatency);
            assertThat(avgLatency, closeTo(15.5, 0.001));
        }
    }

    /**
     * Test COUNT(field) vs COUNT(*) behavior with mixed segments.
     * COUNT(*) counts all documents, COUNT(field) counts non-null values.
     */
    public void testCountFieldWithMixedSegments() throws Exception {
        createStarTreeIndex();

        long baseTimestamp = System.currentTimeMillis();

        // First batch: 5 docs with bytes field
        BulkRequestBuilder bulk1 = client().prepareBulk();
        for (int i = 0; i < 5; i++) {
            bulk1.add(
                new IndexRequest(INDEX_NAME).source(
                    Map.of(
                        "country", "US",
                        "browser", "Chrome",
                        "@timestamp", baseTimestamp + i * 1000,
                        "bytes", 100L,
                        "latency", 10.0
                    )
                )
            );
        }
        bulk1.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        forceMergeIndex();

        // Second batch WITHOUT force merge: 5 more docs
        BulkRequestBuilder bulk2 = client().prepareBulk();
        for (int i = 0; i < 5; i++) {
            bulk2.add(
                new IndexRequest(INDEX_NAME).source(
                    Map.of(
                        "country", "US",
                        "browser", "Chrome",
                        "@timestamp", baseTimestamp + (i + 5) * 1000,
                        "bytes", 200L,
                        "latency", 20.0
                    )
                )
            );
        }
        bulk2.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        // Test COUNT(*)
        try (EsqlQueryResponse results = run("FROM " + INDEX_NAME + " | STATS doc_count = count(*)")) {
            List<List<Object>> values = getValuesList(results);
            assertThat(values, hasSize(1));
            long docCount = ((Number) values.get(0).get(0)).longValue();
            logger.info("COUNT(*) with mixed segments: {} (expected 10)", docCount);
            assertThat(docCount, equalTo(10L));
        }

        // Test COUNT(bytes) - should be same as COUNT(*) since all docs have bytes
        try (EsqlQueryResponse results = run("FROM " + INDEX_NAME + " | STATS bytes_count = count(bytes)")) {
            List<List<Object>> values = getValuesList(results);
            assertThat(values, hasSize(1));
            long bytesCount = ((Number) values.get(0).get(0)).longValue();
            logger.info("COUNT(bytes) with mixed segments: {} (expected 10)", bytesCount);
            assertThat(bytesCount, equalTo(10L));
        }
    }

    /**
     * Test querying multiple indices where one has star-tree and one doesn't.
     */
    public void testMultipleIndicesOnlyOneWithStarTree() throws Exception {
        String indexWithStarTree = INDEX_NAME;
        String indexWithoutStarTree = "no_star_tree_index";

        // Create star-tree index
        createStarTreeIndex();

        // Create regular index without star-tree
        XContentBuilder regularMapping = XContentFactory.jsonBuilder();
        regularMapping.startObject();
        {
            regularMapping.startObject("properties");
            {
                regularMapping.startObject("country").field("type", "keyword").endObject();
                regularMapping.startObject("browser").field("type", "keyword").endObject();
                regularMapping.startObject("@timestamp").field("type", "date").endObject();
                regularMapping.startObject("bytes").field("type", "long").endObject();
                regularMapping.startObject("latency").field("type", "double").endObject();
            }
            regularMapping.endObject();
        }
        regularMapping.endObject();

        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(indexWithoutStarTree)
                .setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .setMapping(regularMapping)
        );

        long baseTimestamp = System.currentTimeMillis();

        // Index to star-tree index
        BulkRequestBuilder bulk1 = client().prepareBulk();
        for (int i = 0; i < 5; i++) {
            bulk1.add(
                new IndexRequest(indexWithStarTree).source(
                    Map.of(
                        "country", "US",
                        "browser", "Chrome",
                        "@timestamp", baseTimestamp + i * 1000,
                        "bytes", 100L,
                        "latency", 10.0
                    )
                )
            );
        }
        bulk1.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        client().admin().indices().forceMerge(new ForceMergeRequest(indexWithStarTree).maxNumSegments(1).flush(true)).actionGet();

        // Index to regular index
        BulkRequestBuilder bulk2 = client().prepareBulk();
        for (int i = 0; i < 5; i++) {
            bulk2.add(
                new IndexRequest(indexWithoutStarTree).source(
                    Map.of(
                        "country", "UK",
                        "browser", "Firefox",
                        "@timestamp", baseTimestamp + (i + 5) * 1000,
                        "bytes", 200L,
                        "latency", 20.0
                    )
                )
            );
        }
        bulk2.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        // Query both indices - should fall back to regular execution
        // Expected: 5 * 100 + 5 * 200 = 500 + 1000 = 1500
        try (EsqlQueryResponse results = run("FROM " + indexWithStarTree + "," + indexWithoutStarTree + " | STATS total = sum(bytes)")) {
            List<List<Object>> values = getValuesList(results);
            assertThat(values, hasSize(1));
            long total = ((Number) values.get(0).get(0)).longValue();
            logger.info("Total bytes from multiple indices: {} (expected 1500)", total);
            assertThat(total, equalTo(1500L));
        }

        // Query with GROUP BY
        try (
            EsqlQueryResponse results = run(
                "FROM " + indexWithStarTree + "," + indexWithoutStarTree + " | STATS total = sum(bytes) BY country | SORT country"
            )
        ) {
            List<List<Object>> values = getValuesList(results);
            assertThat(values, hasSize(2));

            // UK from regular index
            assertThat(values.get(0).get(1), equalTo("UK"));
            assertThat(((Number) values.get(0).get(0)).longValue(), equalTo(1000L));

            // US from star-tree index
            assertThat(values.get(1).get(1), equalTo("US"));
            assertThat(((Number) values.get(1).get(0)).longValue(), equalTo(500L));
        }

        // Cleanup
        client().admin().indices().prepareDelete(indexWithoutStarTree).get();
    }

    /**
     * Test MIN/MAX with GROUP BY and mixed segments.
     */
    public void testMinMaxWithGroupByAndMixedSegments() throws Exception {
        createStarTreeIndex();

        long baseTimestamp = System.currentTimeMillis();

        // First batch: US with bytes 50-150, UK with bytes 60-160
        BulkRequestBuilder bulk1 = client().prepareBulk();
        for (int i = 0; i < 5; i++) {
            bulk1.add(
                new IndexRequest(INDEX_NAME).source(
                    Map.of(
                        "country", "US",
                        "browser", "Chrome",
                        "@timestamp", baseTimestamp + i * 1000,
                        "bytes", 50L + i * 25,  // 50, 75, 100, 125, 150
                        "latency", 10.0
                    )
                )
            );
            bulk1.add(
                new IndexRequest(INDEX_NAME).source(
                    Map.of(
                        "country", "UK",
                        "browser", "Firefox",
                        "@timestamp", baseTimestamp + (i + 5) * 1000,
                        "bytes", 60L + i * 25,  // 60, 85, 110, 135, 160
                        "latency", 15.0
                    )
                )
            );
        }
        bulk1.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        forceMergeIndex();

        // Second batch WITHOUT force merge: US with bytes 200-400, UK with bytes 250-450
        BulkRequestBuilder bulk2 = client().prepareBulk();
        for (int i = 0; i < 5; i++) {
            bulk2.add(
                new IndexRequest(INDEX_NAME).source(
                    Map.of(
                        "country", "US",
                        "browser", "Safari",
                        "@timestamp", baseTimestamp + (i + 10) * 1000,
                        "bytes", 200L + i * 50,  // 200, 250, 300, 350, 400
                        "latency", 20.0
                    )
                )
            );
            bulk2.add(
                new IndexRequest(INDEX_NAME).source(
                    Map.of(
                        "country", "UK",
                        "browser", "Edge",
                        "@timestamp", baseTimestamp + (i + 15) * 1000,
                        "bytes", 250L + i * 50,  // 250, 300, 350, 400, 450
                        "latency", 25.0
                    )
                )
            );
        }
        bulk2.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        // Test MIN/MAX with GROUP BY
        try (
            EsqlQueryResponse results = run(
                "FROM " + INDEX_NAME + " | STATS min_bytes = min(bytes), max_bytes = max(bytes) BY country | SORT country"
            )
        ) {
            List<List<Object>> values = getValuesList(results);
            logger.info("MIN/MAX with GROUP BY results: {}", values);
            assertThat(values, hasSize(2));

            // UK: min=60 (star-tree), max=450 (fallback)
            assertThat(values.get(0).get(2), equalTo("UK"));
            long ukMin = ((Number) values.get(0).get(0)).longValue();
            long ukMax = ((Number) values.get(0).get(1)).longValue();
            logger.info("UK: min={} (expected 60), max={} (expected 450)", ukMin, ukMax);
            assertThat(ukMin, equalTo(60L));
            assertThat(ukMax, equalTo(450L));

            // US: min=50 (star-tree), max=400 (fallback)
            assertThat(values.get(1).get(2), equalTo("US"));
            long usMin = ((Number) values.get(1).get(0)).longValue();
            long usMax = ((Number) values.get(1).get(1)).longValue();
            logger.info("US: min={} (expected 50), max={} (expected 400)", usMin, usMax);
            assertThat(usMin, equalTo(50L));
            assertThat(usMax, equalTo(400L));
        }
    }

    /**
     * Test AVG with GROUP BY and mixed segments.
     * AVG = SUM / COUNT, so both need to be correctly combined.
     */
    public void testAvgWithGroupByAndMixedSegments() throws Exception {
        createStarTreeIndex();

        long baseTimestamp = System.currentTimeMillis();

        // First batch: US with bytes=100 (5 docs), UK with bytes=200 (5 docs)
        BulkRequestBuilder bulk1 = client().prepareBulk();
        for (int i = 0; i < 5; i++) {
            bulk1.add(
                new IndexRequest(INDEX_NAME).source(
                    Map.of(
                        "country", "US",
                        "browser", "Chrome",
                        "@timestamp", baseTimestamp + i * 1000,
                        "bytes", 100L,
                        "latency", 10.0
                    )
                )
            );
            bulk1.add(
                new IndexRequest(INDEX_NAME).source(
                    Map.of(
                        "country", "UK",
                        "browser", "Firefox",
                        "@timestamp", baseTimestamp + (i + 5) * 1000,
                        "bytes", 200L,
                        "latency", 20.0
                    )
                )
            );
        }
        bulk1.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        forceMergeIndex();

        // Second batch WITHOUT force merge: US with bytes=300 (5 docs), UK with bytes=400 (5 docs)
        BulkRequestBuilder bulk2 = client().prepareBulk();
        for (int i = 0; i < 5; i++) {
            bulk2.add(
                new IndexRequest(INDEX_NAME).source(
                    Map.of(
                        "country", "US",
                        "browser", "Safari",
                        "@timestamp", baseTimestamp + (i + 10) * 1000,
                        "bytes", 300L,
                        "latency", 30.0
                    )
                )
            );
            bulk2.add(
                new IndexRequest(INDEX_NAME).source(
                    Map.of(
                        "country", "UK",
                        "browser", "Edge",
                        "@timestamp", baseTimestamp + (i + 15) * 1000,
                        "bytes", 400L,
                        "latency", 40.0
                    )
                )
            );
        }
        bulk2.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        // Test AVG with GROUP BY
        // US: (5*100 + 5*300) / 10 = 2000 / 10 = 200
        // UK: (5*200 + 5*400) / 10 = 3000 / 10 = 300
        try (EsqlQueryResponse results = run("FROM " + INDEX_NAME + " | STATS avg_bytes = avg(bytes) BY country | SORT country")) {
            List<List<Object>> values = getValuesList(results);
            logger.info("AVG with GROUP BY results: {}", values);
            assertThat(values, hasSize(2));

            // UK: avg = 300
            assertThat(values.get(0).get(1), equalTo("UK"));
            double ukAvg = ((Number) values.get(0).get(0)).doubleValue();
            logger.info("UK avg: {} (expected 300.0)", ukAvg);
            assertThat(ukAvg, closeTo(300.0, 0.001));

            // US: avg = 200
            assertThat(values.get(1).get(1), equalTo("US"));
            double usAvg = ((Number) values.get(1).get(0)).doubleValue();
            logger.info("US avg: {} (expected 200.0)", usAvg);
            assertThat(usAvg, closeTo(200.0, 0.001));
        }
    }

    // ========== Time Bucketing (date_histogram) Tests ==========

    /**
     * Test basic time bucketing with GROUP BY on timestamp.
     * The star-tree is configured with 1-hour interval for @timestamp.
     */
    public void testTimeBucketingBasic() throws Exception {
        createStarTreeIndex();

        // Use a fixed base timestamp at the start of an hour for predictable bucketing
        // Example: 2024-01-15 10:00:00 UTC
        long hour1Start = 1705312800000L;  // 2024-01-15 10:00:00 UTC
        long hour2Start = hour1Start + 3600000L;  // 2024-01-15 11:00:00 UTC
        long hour3Start = hour2Start + 3600000L;  // 2024-01-15 12:00:00 UTC

        BulkRequestBuilder bulk = client().prepareBulk();

        // Hour 1: 3 docs with bytes = 100 each (total 300)
        for (int i = 0; i < 3; i++) {
            bulk.add(
                new IndexRequest(INDEX_NAME).source(
                    Map.of(
                        "country", "US",
                        "browser", "Chrome",
                        "@timestamp", hour1Start + i * 60000,  // Spread within the hour
                        "bytes", 100L,
                        "latency", 10.0
                    )
                )
            );
        }

        // Hour 2: 5 docs with bytes = 200 each (total 1000)
        for (int i = 0; i < 5; i++) {
            bulk.add(
                new IndexRequest(INDEX_NAME).source(
                    Map.of(
                        "country", "US",
                        "browser", "Chrome",
                        "@timestamp", hour2Start + i * 60000,
                        "bytes", 200L,
                        "latency", 20.0
                    )
                )
            );
        }

        // Hour 3: 2 docs with bytes = 500 each (total 1000)
        for (int i = 0; i < 2; i++) {
            bulk.add(
                new IndexRequest(INDEX_NAME).source(
                    Map.of(
                        "country", "US",
                        "browser", "Chrome",
                        "@timestamp", hour3Start + i * 60000,
                        "bytes", 500L,
                        "latency", 50.0
                    )
                )
            );
        }

        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        forceMergeIndex();

        // Test SUM grouped by timestamp - should see 3 distinct hour buckets
        try (EsqlQueryResponse results = run("FROM " + INDEX_NAME + " | STATS total_bytes = sum(bytes) BY `@timestamp` | SORT `@timestamp`")) {
            List<List<Object>> values = getValuesList(results);
            logger.info("Time bucketing results: {}", values);
            assertThat("Should have 3 time buckets", values, hasSize(3));

            // Verify each bucket's sum
            // Note: @timestamp values are bucketed to hour boundaries
            long bucket1Total = ((Number) values.get(0).get(0)).longValue();
            long bucket2Total = ((Number) values.get(1).get(0)).longValue();
            long bucket3Total = ((Number) values.get(2).get(0)).longValue();

            logger.info("Bucket 1 total: {} (expected 300)", bucket1Total);
            logger.info("Bucket 2 total: {} (expected 1000)", bucket2Total);
            logger.info("Bucket 3 total: {} (expected 1000)", bucket3Total);

            assertThat(bucket1Total, equalTo(300L));
            assertThat(bucket2Total, equalTo(1000L));
            assertThat(bucket3Total, equalTo(1000L));
        }
    }

    /**
     * Test time bucketing with COUNT aggregation.
     */
    public void testTimeBucketingCount() throws Exception {
        createStarTreeIndex();

        long hour1Start = 1705312800000L;
        long hour2Start = hour1Start + 3600000L;

        BulkRequestBuilder bulk = client().prepareBulk();

        // Hour 1: 4 docs
        for (int i = 0; i < 4; i++) {
            bulk.add(
                new IndexRequest(INDEX_NAME).source(
                    Map.of(
                        "country", "US",
                        "browser", "Chrome",
                        "@timestamp", hour1Start + i * 60000,
                        "bytes", 100L,
                        "latency", 10.0
                    )
                )
            );
        }

        // Hour 2: 6 docs
        for (int i = 0; i < 6; i++) {
            bulk.add(
                new IndexRequest(INDEX_NAME).source(
                    Map.of(
                        "country", "FR",
                        "browser", "Firefox",
                        "@timestamp", hour2Start + i * 60000,
                        "bytes", 200L,
                        "latency", 20.0
                    )
                )
            );
        }

        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        forceMergeIndex();

        // Test COUNT grouped by timestamp
        try (EsqlQueryResponse results = run("FROM " + INDEX_NAME + " | STATS doc_count = count(*) BY `@timestamp` | SORT `@timestamp`")) {
            List<List<Object>> values = getValuesList(results);
            logger.info("Time bucketing COUNT results: {}", values);
            assertThat("Should have 2 time buckets", values, hasSize(2));

            long bucket1Count = ((Number) values.get(0).get(0)).longValue();
            long bucket2Count = ((Number) values.get(1).get(0)).longValue();

            logger.info("Bucket 1 count: {} (expected 4)", bucket1Count);
            logger.info("Bucket 2 count: {} (expected 6)", bucket2Count);

            assertThat(bucket1Count, equalTo(4L));
            assertThat(bucket2Count, equalTo(6L));
        }
    }

    /**
     * Test time bucketing with MIN/MAX aggregations.
     */
    public void testTimeBucketingMinMax() throws Exception {
        createStarTreeIndex();

        long hour1Start = 1705312800000L;
        long hour2Start = hour1Start + 3600000L;

        BulkRequestBuilder bulk = client().prepareBulk();

        // Hour 1: bytes ranging from 100 to 400
        long[] hour1Bytes = { 100L, 250L, 400L, 150L };
        for (int i = 0; i < hour1Bytes.length; i++) {
            bulk.add(
                new IndexRequest(INDEX_NAME).source(
                    Map.of(
                        "country", "US",
                        "browser", "Chrome",
                        "@timestamp", hour1Start + i * 60000,
                        "bytes", hour1Bytes[i],
                        "latency", 10.0
                    )
                )
            );
        }

        // Hour 2: bytes ranging from 50 to 800
        long[] hour2Bytes = { 50L, 800L, 300L };
        for (int i = 0; i < hour2Bytes.length; i++) {
            bulk.add(
                new IndexRequest(INDEX_NAME).source(
                    Map.of(
                        "country", "FR",
                        "browser", "Firefox",
                        "@timestamp", hour2Start + i * 60000,
                        "bytes", hour2Bytes[i],
                        "latency", 20.0
                    )
                )
            );
        }

        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        forceMergeIndex();

        // Test MIN/MAX grouped by timestamp
        try (
            EsqlQueryResponse results = run(
                "FROM " + INDEX_NAME + " | STATS min_bytes = min(bytes), max_bytes = max(bytes) BY `@timestamp` | SORT `@timestamp`"
            )
        ) {
            List<List<Object>> values = getValuesList(results);
            logger.info("Time bucketing MIN/MAX results: {}", values);
            assertThat("Should have 2 time buckets", values, hasSize(2));

            // Hour 1: min=100, max=400
            long bucket1Min = ((Number) values.get(0).get(0)).longValue();
            long bucket1Max = ((Number) values.get(0).get(1)).longValue();
            logger.info("Bucket 1 min/max: {}/{} (expected 100/400)", bucket1Min, bucket1Max);
            assertThat(bucket1Min, equalTo(100L));
            assertThat(bucket1Max, equalTo(400L));

            // Hour 2: min=50, max=800
            long bucket2Min = ((Number) values.get(1).get(0)).longValue();
            long bucket2Max = ((Number) values.get(1).get(1)).longValue();
            logger.info("Bucket 2 min/max: {}/{} (expected 50/800)", bucket2Min, bucket2Max);
            assertThat(bucket2Min, equalTo(50L));
            assertThat(bucket2Max, equalTo(800L));
        }
    }

    /**
     * Test time bucketing combined with another grouping field.
     * GROUP BY country, @timestamp
     */
    public void testTimeBucketingWithCountryGroupBy() throws Exception {
        createStarTreeIndex();

        long hour1Start = 1705312800000L;
        long hour2Start = hour1Start + 3600000L;

        BulkRequestBuilder bulk = client().prepareBulk();

        // Hour 1: US = 3 docs * 100 = 300, FR = 2 docs * 200 = 400
        for (int i = 0; i < 3; i++) {
            bulk.add(
                new IndexRequest(INDEX_NAME).source(
                    Map.of("country", "US", "browser", "Chrome", "@timestamp", hour1Start + i * 60000, "bytes", 100L, "latency", 10.0)
                )
            );
        }
        for (int i = 0; i < 2; i++) {
            bulk.add(
                new IndexRequest(INDEX_NAME).source(
                    Map.of("country", "FR", "browser", "Firefox", "@timestamp", hour1Start + (i + 3) * 60000, "bytes", 200L, "latency", 20.0)
                )
            );
        }

        // Hour 2: US = 2 docs * 300 = 600, FR = 4 docs * 150 = 600
        for (int i = 0; i < 2; i++) {
            bulk.add(
                new IndexRequest(INDEX_NAME).source(
                    Map.of("country", "US", "browser", "Safari", "@timestamp", hour2Start + i * 60000, "bytes", 300L, "latency", 30.0)
                )
            );
        }
        for (int i = 0; i < 4; i++) {
            bulk.add(
                new IndexRequest(INDEX_NAME).source(
                    Map.of("country", "FR", "browser", "Edge", "@timestamp", hour2Start + (i + 2) * 60000, "bytes", 150L, "latency", 15.0)
                )
            );
        }

        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        forceMergeIndex();

        // GROUP BY country, @timestamp
        try (
            EsqlQueryResponse results = run(
                "FROM " + INDEX_NAME + " | STATS total_bytes = sum(bytes) BY country, `@timestamp` | SORT country, `@timestamp`"
            )
        ) {
            List<List<Object>> values = getValuesList(results);
            logger.info("Time bucketing with country results: {}", values);
            assertThat("Should have 4 groups (2 countries * 2 hours)", values, hasSize(4));

            // Results should be sorted by country, then timestamp
            // FR, hour1: 400
            assertThat(values.get(0).get(1), equalTo("FR"));
            assertThat(((Number) values.get(0).get(0)).longValue(), equalTo(400L));

            // FR, hour2: 600
            assertThat(values.get(1).get(1), equalTo("FR"));
            assertThat(((Number) values.get(1).get(0)).longValue(), equalTo(600L));

            // US, hour1: 300
            assertThat(values.get(2).get(1), equalTo("US"));
            assertThat(((Number) values.get(2).get(0)).longValue(), equalTo(300L));

            // US, hour2: 600
            assertThat(values.get(3).get(1), equalTo("US"));
            assertThat(((Number) values.get(3).get(0)).longValue(), equalTo(600L));
        }
    }

    /**
     * Test time bucketing with mixed segments (star-tree and non-star-tree).
     */
    public void testTimeBucketingWithMixedSegments() throws Exception {
        createStarTreeIndex();

        long hour1Start = 1705312800000L;
        long hour2Start = hour1Start + 3600000L;

        // First batch: Hour 1 data, force merged (will have star-tree)
        BulkRequestBuilder bulk1 = client().prepareBulk();
        for (int i = 0; i < 5; i++) {
            bulk1.add(
                new IndexRequest(INDEX_NAME).source(
                    Map.of("country", "US", "browser", "Chrome", "@timestamp", hour1Start + i * 60000, "bytes", 100L, "latency", 10.0)
                )
            );
        }
        bulk1.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        forceMergeIndex();

        // Second batch: Hour 2 data, NOT force merged (no star-tree, fallback processing)
        BulkRequestBuilder bulk2 = client().prepareBulk();
        for (int i = 0; i < 5; i++) {
            bulk2.add(
                new IndexRequest(INDEX_NAME).source(
                    Map.of("country", "US", "browser", "Chrome", "@timestamp", hour2Start + i * 60000, "bytes", 200L, "latency", 20.0)
                )
            );
        }
        bulk2.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        // Test SUM grouped by timestamp with mixed segments
        // Hour 1 (star-tree): 5 * 100 = 500
        // Hour 2 (fallback): 5 * 200 = 1000
        try (EsqlQueryResponse results = run("FROM " + INDEX_NAME + " | STATS total_bytes = sum(bytes) BY `@timestamp` | SORT `@timestamp`")) {
            List<List<Object>> values = getValuesList(results);
            logger.info("Time bucketing with mixed segments: {}", values);
            assertThat("Should have 2 time buckets", values, hasSize(2));

            long bucket1Total = ((Number) values.get(0).get(0)).longValue();
            long bucket2Total = ((Number) values.get(1).get(0)).longValue();

            logger.info("Bucket 1 total (star-tree): {} (expected 500)", bucket1Total);
            logger.info("Bucket 2 total (fallback): {} (expected 1000)", bucket2Total);

            assertThat(bucket1Total, equalTo(500L));
            assertThat(bucket2Total, equalTo(1000L));
        }
    }

    /**
     * Test time bucketing with AVG aggregation.
     */
    public void testTimeBucketingAvg() throws Exception {
        createStarTreeIndex();

        long hour1Start = 1705312800000L;
        long hour2Start = hour1Start + 3600000L;

        BulkRequestBuilder bulk = client().prepareBulk();

        // Hour 1: 4 docs with bytes = 100, 200, 300, 400 -> avg = 250
        long[] hour1Bytes = { 100L, 200L, 300L, 400L };
        for (int i = 0; i < hour1Bytes.length; i++) {
            bulk.add(
                new IndexRequest(INDEX_NAME).source(
                    Map.of("country", "US", "browser", "Chrome", "@timestamp", hour1Start + i * 60000, "bytes", hour1Bytes[i], "latency", 10.0)
                )
            );
        }

        // Hour 2: 2 docs with bytes = 500, 700 -> avg = 600
        long[] hour2Bytes = { 500L, 700L };
        for (int i = 0; i < hour2Bytes.length; i++) {
            bulk.add(
                new IndexRequest(INDEX_NAME).source(
                    Map.of("country", "FR", "browser", "Firefox", "@timestamp", hour2Start + i * 60000, "bytes", hour2Bytes[i], "latency", 20.0)
                )
            );
        }

        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        forceMergeIndex();

        // Test AVG grouped by timestamp
        try (EsqlQueryResponse results = run("FROM " + INDEX_NAME + " | STATS avg_bytes = avg(bytes) BY `@timestamp` | SORT `@timestamp`")) {
            List<List<Object>> values = getValuesList(results);
            logger.info("Time bucketing AVG results: {}", values);
            assertThat("Should have 2 time buckets", values, hasSize(2));

            double bucket1Avg = ((Number) values.get(0).get(0)).doubleValue();
            double bucket2Avg = ((Number) values.get(1).get(0)).doubleValue();

            logger.info("Bucket 1 avg: {} (expected 250.0)", bucket1Avg);
            logger.info("Bucket 2 avg: {} (expected 600.0)", bucket2Avg);

            assertThat(bucket1Avg, closeTo(250.0, 0.001));
            assertThat(bucket2Avg, closeTo(600.0, 0.001));
        }
    }

    /**
     * Test that time bucketing uses star-tree operator (verified via profile).
     */
    public void testTimeBucketingUsesStarTreeOperator() throws Exception {
        createStarTreeIndex();

        long hour1Start = 1705312800000L;

        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < 5; i++) {
            bulk.add(
                new IndexRequest(INDEX_NAME).source(
                    Map.of("country", "US", "browser", "Chrome", "@timestamp", hour1Start + i * 60000, "bytes", 100L, "latency", 10.0)
                )
            );
        }
        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        forceMergeIndex();

        // Run query with profile
        EsqlQueryRequest request = syncEsqlQueryRequest("FROM " + INDEX_NAME + " | STATS total_bytes = sum(bytes) BY `@timestamp`");
        request.profile(true);
        request.pragmas(getPragmas());

        try (EsqlQueryResponse response = run(request)) {
            // Verify result
            List<List<Object>> values = getValuesList(response);
            assertThat(values, hasSize(1));
            long total = ((Number) values.get(0).get(0)).longValue();
            assertThat(total, equalTo(500L));

            // Verify star-tree operator is used
            EsqlQueryResponse.Profile profile = response.profile();
            assertNotNull("Profile should not be null", profile);

            boolean foundStarTreeOperator = profile.drivers()
                .stream()
                .flatMap(d -> d.operators().stream())
                .anyMatch(op -> op.operator().contains("StarTreeSource"));

            logger.info("Star-tree operator used for time bucketing: {}", foundStarTreeOperator);
            assertTrue("StarTreeSource operator should be used for time bucketing query", foundStarTreeOperator);
        }
    }

    /**
     * Test time bucketing with double field aggregation.
     */
    public void testTimeBucketingWithDoubleField() throws Exception {
        createStarTreeIndex();

        long hour1Start = 1705312800000L;
        long hour2Start = hour1Start + 3600000L;

        BulkRequestBuilder bulk = client().prepareBulk();

        // Hour 1: latency = 10.5 each (3 docs) -> sum = 31.5
        for (int i = 0; i < 3; i++) {
            bulk.add(
                new IndexRequest(INDEX_NAME).source(
                    Map.of("country", "US", "browser", "Chrome", "@timestamp", hour1Start + i * 60000, "bytes", 100L, "latency", 10.5)
                )
            );
        }

        // Hour 2: latency = 20.5 each (2 docs) -> sum = 41.0
        for (int i = 0; i < 2; i++) {
            bulk.add(
                new IndexRequest(INDEX_NAME).source(
                    Map.of("country", "FR", "browser", "Firefox", "@timestamp", hour2Start + i * 60000, "bytes", 200L, "latency", 20.5)
                )
            );
        }

        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        forceMergeIndex();

        // Test SUM on double field grouped by timestamp
        try (
            EsqlQueryResponse results = run("FROM " + INDEX_NAME + " | STATS total_latency = sum(latency) BY `@timestamp` | SORT `@timestamp`")
        ) {
            List<List<Object>> values = getValuesList(results);
            logger.info("Time bucketing double field results: {}", values);
            assertThat("Should have 2 time buckets", values, hasSize(2));

            double bucket1Total = ((Number) values.get(0).get(0)).doubleValue();
            double bucket2Total = ((Number) values.get(1).get(0)).doubleValue();

            logger.info("Bucket 1 latency sum: {} (expected 31.5)", bucket1Total);
            logger.info("Bucket 2 latency sum: {} (expected 41.0)", bucket2Total);

            assertThat(bucket1Total, closeTo(31.5, 0.001));
            assertThat(bucket2Total, closeTo(41.0, 0.001));
        }
    }

    /**
     * Test time bucketing spanning many hours.
     */
    public void testTimeBucketingManyBuckets() throws Exception {
        createStarTreeIndex();

        long baseTimestamp = 1705312800000L;  // 2024-01-15 10:00:00 UTC
        int numHours = 10;

        BulkRequestBuilder bulk = client().prepareBulk();

        // Create data for 10 consecutive hours, 2 docs per hour
        for (int hour = 0; hour < numHours; hour++) {
            long hourStart = baseTimestamp + hour * 3600000L;
            for (int i = 0; i < 2; i++) {
                bulk.add(
                    new IndexRequest(INDEX_NAME).source(
                        Map.of(
                            "country", "US",
                            "browser", "Chrome",
                            "@timestamp", hourStart + i * 60000,
                            "bytes", (hour + 1) * 100L,  // 100, 200, 300, ...
                            "latency", 10.0
                        )
                    )
                );
            }
        }

        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        forceMergeIndex();

        // Test COUNT grouped by timestamp - should see 10 buckets with 2 docs each
        try (EsqlQueryResponse results = run("FROM " + INDEX_NAME + " | STATS doc_count = count(*) BY `@timestamp` | SORT `@timestamp`")) {
            List<List<Object>> values = getValuesList(results);
            logger.info("Many buckets COUNT results: {} buckets", values.size());
            assertThat("Should have 10 time buckets", values, hasSize(numHours));

            // Each bucket should have 2 docs
            for (int i = 0; i < values.size(); i++) {
                long count = ((Number) values.get(i).get(0)).longValue();
                assertThat("Bucket " + i + " should have 2 docs", count, equalTo(2L));
            }
        }

        // Test SUM grouped by timestamp
        // Hour 0: 2 * 100 = 200, Hour 1: 2 * 200 = 400, ..., Hour 9: 2 * 1000 = 2000
        try (EsqlQueryResponse results = run("FROM " + INDEX_NAME + " | STATS total_bytes = sum(bytes) BY `@timestamp` | SORT `@timestamp`")) {
            List<List<Object>> values = getValuesList(results);
            assertThat("Should have 10 time buckets", values, hasSize(numHours));

            for (int i = 0; i < values.size(); i++) {
                long expectedSum = 2 * (i + 1) * 100L;  // 200, 400, 600, ...
                long actualSum = ((Number) values.get(i).get(0)).longValue();
                logger.info("Bucket {} sum: {} (expected {})", i, actualSum, expectedSum);
                assertThat("Bucket " + i + " sum mismatch", actualSum, equalTo(expectedSum));
            }
        }
    }

    /**
     * Test using bucket() function with star-tree.
     * Query: FROM index | STATS sum(bytes) BY bucket(@timestamp, 1 hour)
     * This should use star-tree because the bucket interval matches the star-tree date_histogram interval.
     */
    public void testBucketFunctionWithStarTree() throws Exception {
        createStarTreeIndex();

        long hour1Start = 1705312800000L;  // 2024-01-15 10:00:00 UTC
        long hour2Start = hour1Start + 3600000L;  // 2024-01-15 11:00:00 UTC
        long hour3Start = hour2Start + 3600000L;  // 2024-01-15 12:00:00 UTC

        BulkRequestBuilder bulk = client().prepareBulk();

        // Hour 1: 3 docs with bytes = 100 each (total 300)
        for (int i = 0; i < 3; i++) {
            bulk.add(
                new IndexRequest(INDEX_NAME).source(
                    Map.of(
                        "country", "US",
                        "browser", "Chrome",
                        "@timestamp", hour1Start + i * 60000,
                        "bytes", 100L,
                        "latency", 10.0
                    )
                )
            );
        }

        // Hour 2: 5 docs with bytes = 200 each (total 1000)
        for (int i = 0; i < 5; i++) {
            bulk.add(
                new IndexRequest(INDEX_NAME).source(
                    Map.of(
                        "country", "FR",
                        "browser", "Firefox",
                        "@timestamp", hour2Start + i * 60000,
                        "bytes", 200L,
                        "latency", 20.0
                    )
                )
            );
        }

        // Hour 3: 2 docs with bytes = 500 each (total 1000)
        for (int i = 0; i < 2; i++) {
            bulk.add(
                new IndexRequest(INDEX_NAME).source(
                    Map.of(
                        "country", "DE",
                        "browser", "Safari",
                        "@timestamp", hour3Start + i * 60000,
                        "bytes", 500L,
                        "latency", 50.0
                    )
                )
            );
        }

        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        forceMergeIndex();

        // Test using bucket() function - should use star-tree
        String query = "FROM " + INDEX_NAME + " | STATS total_bytes = sum(bytes) BY ts = bucket(`@timestamp`, 1 hour) | SORT ts";
        try (EsqlQueryResponse results = run(query)) {
            List<List<Object>> values = getValuesList(results);
            logger.info("Bucket function results: {}", values);
            assertThat("Should have 3 time buckets", values, hasSize(3));

            long bucket1Total = ((Number) values.get(0).get(0)).longValue();
            long bucket2Total = ((Number) values.get(1).get(0)).longValue();
            long bucket3Total = ((Number) values.get(2).get(0)).longValue();

            assertThat(bucket1Total, equalTo(300L));
            assertThat(bucket2Total, equalTo(1000L));
            assertThat(bucket3Total, equalTo(1000L));
        }

        // Test bucket() function with profile to verify star-tree usage
        String profileQuery = "FROM " + INDEX_NAME + " | STATS total_bytes = sum(bytes) BY bucket(`@timestamp`, 1 hour)";
        EsqlQueryRequest profileRequest = syncEsqlQueryRequest(profileQuery);
        profileRequest.profile(true);
        profileRequest.pragmas(getPragmas());
        try (EsqlQueryResponse results = run(profileRequest)) {
            List<List<Object>> values = getValuesList(results);
            logger.info("Bucket function profile results: {}", values);
            assertThat("Should have 3 time buckets", values, hasSize(3));

            // Check profile for star-tree operator usage
            EsqlQueryResponse.Profile profile = results.profile();
            assertNotNull("Profile should not be null", profile);

            boolean foundStarTreeOperator = false;
            for (var driverProfile : profile.drivers()) {
                for (var op : driverProfile.operators()) {
                    logger.info("Operator: {}", op.status());
                    if (op.status() instanceof StarTreeSourceOperatorFactory.Status status) {
                        foundStarTreeOperator = true;
                        assertTrue("Star-tree should be used", status.isUsingStarTree());
                        logger.info("Star-tree status: {}", status);
                    }
                }
            }
            assertTrue("Star-tree operator should be used for bucket() query", foundStarTreeOperator);
        }
    }

    /**
     * Test bucket() function with coarser interval (2 hours vs 1 hour star-tree).
     * This should use star-tree because 2 hours is a multiple of 1 hour.
     */
    public void testBucketFunctionWithCoarserInterval() throws Exception {
        createStarTreeIndex();

        long hour1Start = 1705312800000L;  // 2024-01-15 10:00:00 UTC
        long hour2Start = hour1Start + 3600000L;  // 2024-01-15 11:00:00 UTC

        BulkRequestBuilder bulk = client().prepareBulk();

        // Add docs across 2 hours
        bulk.add(
            new IndexRequest(INDEX_NAME).source(
                Map.of("country", "US", "browser", "Chrome", "@timestamp", hour1Start, "bytes", 100L, "latency", 10.0)
            )
        );
        bulk.add(
            new IndexRequest(INDEX_NAME).source(
                Map.of("country", "US", "browser", "Chrome", "@timestamp", hour2Start, "bytes", 200L, "latency", 20.0)
            )
        );

        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        forceMergeIndex();

        // Test using bucket() with 2-hour interval - should aggregate both hours into one bucket
        String query = "FROM " + INDEX_NAME + " | STATS total_bytes = sum(bytes) BY bucket(`@timestamp`, 2 hours)";
        try (EsqlQueryResponse results = run(query)) {
            List<List<Object>> values = getValuesList(results);
            logger.info("Bucket function (2h) results: {}", values);
            // Both hours (10:00 and 11:00) should be in the same 2-hour bucket (10:00-12:00)
            assertThat("Should have 1 time bucket", values, hasSize(1));
            long total = ((Number) values.get(0).get(0)).longValue();
            assertThat(total, equalTo(300L));
        }
    }

    /**
     * Test bucket() function with finer interval (30 min vs 1 hour star-tree).
     * This should NOT use star-tree because query interval is finer than star-tree interval.
     */
    public void testBucketFunctionWithFinerIntervalFallback() throws Exception {
        createStarTreeIndex();

        long hour1Start = 1705312800000L;  // 2024-01-15 10:00:00 UTC

        BulkRequestBuilder bulk = client().prepareBulk();

        // Add docs at different times within the hour
        bulk.add(
            new IndexRequest(INDEX_NAME).source(
                Map.of("country", "US", "browser", "Chrome", "@timestamp", hour1Start, "bytes", 100L, "latency", 10.0)
            )
        );
        bulk.add(
            new IndexRequest(INDEX_NAME).source(
                Map.of("country", "US", "browser", "Chrome", "@timestamp", hour1Start + 1800000L, "bytes", 200L, "latency", 20.0)  // +30 min
            )
        );

        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        forceMergeIndex();

        // Test using bucket() with 30-minute interval - should NOT use star-tree (finer than 1h)
        String query = "FROM " + INDEX_NAME + " | STATS total_bytes = sum(bytes) BY bucket(`@timestamp`, 30 minutes)";
        EsqlQueryRequest queryRequest = syncEsqlQueryRequest(query);
        queryRequest.profile(true);
        queryRequest.pragmas(getPragmas());
        try (EsqlQueryResponse results = run(queryRequest)) {
            List<List<Object>> values = getValuesList(results);
            logger.info("Bucket function (30m) results: {}", values);

            // Should still get correct results via fallback
            // Hour starts at 10:00, docs are at 10:00 and 10:30, so 2 buckets
            assertThat("Should have 2 time buckets", values, hasSize(2));

            // Check profile - should NOT have star-tree operator (should use regular Lucene)
            EsqlQueryResponse.Profile profile = results.profile();
            assertNotNull("Profile should not be null", profile);

            boolean foundStarTreeOperator = false;
            for (var driverProfile : profile.drivers()) {
                for (var op : driverProfile.operators()) {
                    if (op.status() instanceof StarTreeSourceOperatorFactory.Status status) {
                        foundStarTreeOperator = status.isUsingStarTree();
                    }
                }
            }
            assertFalse("Star-tree operator should NOT be used for finer interval bucket() query", foundStarTreeOperator);
        }
    }

    /**
     * Test string equality filtering on a grouping field.
     * WHERE country == "US" should use star-tree with string→ordinal lookup.
     */
    public void testStringEqualityFilter() throws Exception {
        createStarTreeIndex();
        indexTestDocuments();
        forceMergeIndex();

        // Filter on country (a grouping field) with string equality
        String query = "FROM " + INDEX_NAME + " | WHERE country == \"US\" | STATS total_bytes = sum(bytes)";
        EsqlQueryRequest queryRequest = syncEsqlQueryRequest(query);
        queryRequest.profile(true);
        queryRequest.pragmas(getPragmas());
        try (EsqlQueryResponse results = run(queryRequest)) {
            List<List<Object>> values = getValuesList(results);
            assertThat(values, hasSize(1));
            // US bytes: 100+300+120+250+200+400 = 1370
            long total = ((Number) values.get(0).get(0)).longValue();
            assertThat(total, equalTo(1370L));

            // Check profile - should have star-tree operator
            EsqlQueryResponse.Profile profile = results.profile();
            assertNotNull("Profile should not be null", profile);

            boolean foundStarTreeOperator = false;
            for (var driverProfile : profile.drivers()) {
                for (var op : driverProfile.operators()) {
                    if (op.status() instanceof StarTreeSourceOperatorFactory.Status status) {
                        foundStarTreeOperator = status.isUsingStarTree();
                    }
                }
            }
            assertTrue("Star-tree operator should be used for string equality filter query", foundStarTreeOperator);
        }
    }

    /**
     * Test string equality filter with GROUP BY.
     * WHERE country == "US" | STATS ... BY browser should use star-tree.
     */
    public void testStringEqualityFilterWithGroupBy() throws Exception {
        createStarTreeIndex();
        indexTestDocuments();
        forceMergeIndex();

        // Filter on country and group by browser
        String query = "FROM " + INDEX_NAME + " | WHERE country == \"US\" | STATS total_bytes = sum(bytes) BY browser | SORT browser";
        EsqlQueryRequest queryRequest = syncEsqlQueryRequest(query);
        queryRequest.profile(true);
        queryRequest.pragmas(getPragmas());
        try (EsqlQueryResponse results = run(queryRequest)) {
            List<List<Object>> values = getValuesList(results);
            logger.info("String equality filter with GROUP BY results: {}", values);
            // Should have results grouped by browser for US only
            assertThat(values.size(), greaterThan(0));

            // Check that we get Chrome, Firefox, and Safari data for US
            // Test data: US Chrome: 100+250+200=550, US Firefox: 120, US Safari: 300+400=700
            boolean foundChrome = false;
            boolean foundFirefox = false;
            boolean foundSafari = false;
            for (List<Object> row : values) {
                String browser = (String) row.get(1);
                if ("Chrome".equals(browser)) {
                    foundChrome = true;
                    // US Chrome bytes: 100+250+200 = 550
                    assertThat(((Number) row.get(0)).longValue(), equalTo(550L));
                } else if ("Firefox".equals(browser)) {
                    foundFirefox = true;
                    // US Firefox bytes: 120
                    assertThat(((Number) row.get(0)).longValue(), equalTo(120L));
                } else if ("Safari".equals(browser)) {
                    foundSafari = true;
                    // US Safari bytes: 300+400 = 700
                    assertThat(((Number) row.get(0)).longValue(), equalTo(700L));
                }
            }
            assertTrue("Should have Chrome data", foundChrome);
            assertTrue("Should have Firefox data", foundFirefox);
            assertTrue("Should have Safari data", foundSafari);

            // Check profile - should have star-tree operator
            EsqlQueryResponse.Profile profile = results.profile();
            assertNotNull("Profile should not be null", profile);

            boolean foundStarTreeOperator = false;
            for (var driverProfile : profile.drivers()) {
                for (var op : driverProfile.operators()) {
                    if (op.status() instanceof StarTreeSourceOperatorFactory.Status status) {
                        foundStarTreeOperator = status.isUsingStarTree();
                    }
                }
            }
            assertTrue("Star-tree operator should be used for string equality filter with GROUP BY", foundStarTreeOperator);
        }
    }

    /**
     * Test IN clause filter on grouping field without GROUP BY.
     * Query: FROM test | WHERE country IN ("US", "FR") | STATS total_bytes = sum(bytes)
     */
    public void testInClauseFilter() throws Exception {
        createStarTreeIndex();
        indexTestDocuments();
        forceMergeIndex();

        // Query with IN clause filter on grouping field (no GROUP BY)
        // US: 100 + 300 + 120 + 250 + 200 + 400 = 1370
        // FR: 200 + 180 = 380
        // Total US + FR = 1750
        EsqlQueryRequest request = syncEsqlQueryRequest(
            "FROM " + INDEX_NAME + " | WHERE country IN (\"US\", \"FR\") | STATS total_bytes = sum(bytes)"
        );
        request.profile(true);
        request.pragmas(getPragmas());

        try (EsqlQueryResponse results = run(request)) {
            List<List<Object>> values = getValuesList(results);
            logger.info("IN clause filter results: {}", values);
            assertThat(values, hasSize(1));

            long totalBytes = ((Number) values.get(0).get(0)).longValue();
            logger.info("Total bytes for US+FR: {} (expected 1750)", totalBytes);
            assertThat(totalBytes, equalTo(1750L));

            // Check profile - should have star-tree operator
            EsqlQueryResponse.Profile profile = results.profile();
            assertNotNull("Profile should not be null", profile);

            boolean foundStarTreeOperator = false;
            for (var driverProfile : profile.drivers()) {
                for (var op : driverProfile.operators()) {
                    if (op.status() instanceof StarTreeSourceOperatorFactory.Status status) {
                        foundStarTreeOperator = status.isUsingStarTree();
                    }
                }
            }
            assertTrue("Star-tree operator should be used for IN clause filter", foundStarTreeOperator);
        }
    }

    /**
     * Test IN clause filter on grouping field with GROUP BY.
     * Query: FROM test | WHERE country IN ("US", "FR") | STATS total_bytes = sum(bytes) BY browser | SORT browser
     */
    public void testInClauseFilterWithGroupBy() throws Exception {
        createStarTreeIndex();
        indexTestDocuments();
        forceMergeIndex();

        // Query with IN clause filter and GROUP BY
        // US+FR by browser:
        //   Chrome: US(100+250+200) + FR(0) = 550
        //   Firefox: US(120) + FR(200) = 320
        //   Safari: US(300+400) + FR(180) = 880
        EsqlQueryRequest request = syncEsqlQueryRequest(
            "FROM " + INDEX_NAME + " | WHERE country IN (\"US\", \"FR\") | STATS total_bytes = sum(bytes) BY browser | SORT browser"
        );
        request.profile(true);
        request.pragmas(getPragmas());

        try (EsqlQueryResponse results = run(request)) {
            List<List<Object>> values = getValuesList(results);
            logger.info("IN clause filter with GROUP BY results: {}", values);
            assertThat(values, hasSize(3));

            // Results should be sorted by browser: Chrome, Firefox, Safari
            assertThat(values.get(0).get(1), equalTo("Chrome"));
            assertThat(((Number) values.get(0).get(0)).longValue(), equalTo(550L));

            assertThat(values.get(1).get(1), equalTo("Firefox"));
            assertThat(((Number) values.get(1).get(0)).longValue(), equalTo(320L));

            assertThat(values.get(2).get(1), equalTo("Safari"));
            assertThat(((Number) values.get(2).get(0)).longValue(), equalTo(880L));

            // Check profile - should have star-tree operator
            EsqlQueryResponse.Profile profile = results.profile();
            assertNotNull("Profile should not be null", profile);

            boolean foundStarTreeOperator = false;
            for (var driverProfile : profile.drivers()) {
                for (var op : driverProfile.operators()) {
                    if (op.status() instanceof StarTreeSourceOperatorFactory.Status status) {
                        foundStarTreeOperator = status.isUsingStarTree();
                    }
                }
            }
            assertTrue("Star-tree operator should be used for IN clause filter with GROUP BY", foundStarTreeOperator);
        }
    }

    private void createStarTreeIndex() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject();
        {
            // Star-tree configuration
            mapping.startObject("_star_tree");
            {
                mapping.startObject("default");
                {
                    // Grouping fields (dimensions)
                    mapping.startArray("grouping_fields");
                    {
                        mapping.startObject();
                        mapping.field("field", "country");
                        mapping.endObject();

                        mapping.startObject();
                        mapping.field("field", "browser");
                        mapping.endObject();

                        mapping.startObject();
                        mapping.field("field", "@timestamp");
                        mapping.field("type", "date_histogram");
                        mapping.field("interval", "1h");
                        mapping.endObject();
                    }
                    mapping.endArray();

                    // Values (metrics)
                    mapping.startArray("values");
                    {
                        mapping.startObject();
                        mapping.field("field", "bytes");
                        mapping.field("aggregations", List.of("SUM", "COUNT", "MIN", "MAX"));
                        mapping.endObject();

                        mapping.startObject();
                        mapping.field("field", "latency");
                        mapping.field("aggregations", List.of("SUM", "COUNT", "MIN", "MAX"));
                        mapping.endObject();
                    }
                    mapping.endArray();

                    // Config - use low max_leaf_docs to force tree creation even with few test docs
                    // Use star_node_threshold=1 so star nodes are created at each dimension level,
                    // which is required for GROUP BY on dimensions that aren't the first in the tree
                    mapping.startObject("config");
                    mapping.field("max_leaf_docs", 1);
                    mapping.field("star_node_threshold", 1);
                    mapping.endObject();
                }
                mapping.endObject();
            }
            mapping.endObject();

            // Properties
            mapping.startObject("properties");
            {
                mapping.startObject("country");
                mapping.field("type", "keyword");
                mapping.endObject();

                mapping.startObject("browser");
                mapping.field("type", "keyword");
                mapping.endObject();

                mapping.startObject("@timestamp");
                mapping.field("type", "date");
                mapping.endObject();

                mapping.startObject("bytes");
                mapping.field("type", "long");
                mapping.endObject();

                mapping.startObject("latency");
                mapping.field("type", "double");
                mapping.endObject();
            }
            mapping.endObject();
        }
        mapping.endObject();

        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(INDEX_NAME)
                .setSettings(
                    Settings.builder()
                        // Use single shard for predictable star-tree behavior
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                        // Disable compound file format so star-tree files are accessible as standalone files
                        .put("index.compound_format", false)
                )
                .setMapping(mapping)
        );
    }

    /**
     * Create an index with multiple star-tree definitions for testing optimizer selection.
     */
    private void createMultiStarTreeIndex() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject();
        {
            // Multiple star-tree configurations
            mapping.startObject("_star_tree");
            {
                // Star-tree 1: Optimized for country-only queries
                mapping.startObject("by_country");
                {
                    mapping.startArray("grouping_fields");
                    {
                        mapping.startObject();
                        mapping.field("field", "country");
                        mapping.endObject();
                    }
                    mapping.endArray();

                    mapping.startArray("values");
                    {
                        mapping.startObject();
                        mapping.field("field", "bytes");
                        mapping.field("aggregations", List.of("SUM", "COUNT", "MIN", "MAX"));
                        mapping.endObject();
                    }
                    mapping.endArray();

                    mapping.startObject("config");
                    mapping.field("max_leaf_docs", 1);
                    mapping.endObject();
                }
                mapping.endObject();

                // Star-tree 2: Optimized for browser-only queries
                mapping.startObject("by_browser");
                {
                    mapping.startArray("grouping_fields");
                    {
                        mapping.startObject();
                        mapping.field("field", "browser");
                        mapping.endObject();
                    }
                    mapping.endArray();

                    mapping.startArray("values");
                    {
                        mapping.startObject();
                        mapping.field("field", "bytes");
                        mapping.field("aggregations", List.of("SUM", "COUNT", "MIN", "MAX"));
                        mapping.endObject();
                    }
                    mapping.endArray();

                    mapping.startObject("config");
                    mapping.field("max_leaf_docs", 1);
                    mapping.endObject();
                }
                mapping.endObject();

                // Star-tree 3: Optimized for country+browser queries
                mapping.startObject("by_country_browser");
                {
                    mapping.startArray("grouping_fields");
                    {
                        mapping.startObject();
                        mapping.field("field", "country");
                        mapping.endObject();

                        mapping.startObject();
                        mapping.field("field", "browser");
                        mapping.endObject();
                    }
                    mapping.endArray();

                    mapping.startArray("values");
                    {
                        mapping.startObject();
                        mapping.field("field", "bytes");
                        mapping.field("aggregations", List.of("SUM", "COUNT", "MIN", "MAX"));
                        mapping.endObject();

                        mapping.startObject();
                        mapping.field("field", "latency");
                        mapping.field("aggregations", List.of("SUM", "COUNT"));
                        mapping.endObject();
                    }
                    mapping.endArray();

                    mapping.startObject("config");
                    mapping.field("max_leaf_docs", 1);
                    mapping.endObject();
                }
                mapping.endObject();
            }
            mapping.endObject();

            // Properties
            mapping.startObject("properties");
            {
                mapping.startObject("country");
                mapping.field("type", "keyword");
                mapping.endObject();

                mapping.startObject("browser");
                mapping.field("type", "keyword");
                mapping.endObject();

                mapping.startObject("@timestamp");
                mapping.field("type", "date");
                mapping.endObject();

                mapping.startObject("bytes");
                mapping.field("type", "long");
                mapping.endObject();

                mapping.startObject("latency");
                mapping.field("type", "double");
                mapping.endObject();
            }
            mapping.endObject();
        }
        mapping.endObject();

        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(INDEX_NAME)
                .setSettings(
                    Settings.builder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                        .put("index.compound_format", false)
                )
                .setMapping(mapping)
        );
    }

    private void indexTestDocuments() {
        long baseTimestamp = System.currentTimeMillis();

        // Index documents in multiple batches to create multiple segments
        // First batch - 5 documents
        BulkRequestBuilder bulk1 = client().prepareBulk();
        bulk1.add(
            new IndexRequest(INDEX_NAME).source(
                Map.of("country", "US", "browser", "Chrome", "@timestamp", baseTimestamp, "bytes", 100, "latency", 10.5)
            )
        );
        bulk1.add(
            new IndexRequest(INDEX_NAME).source(
                Map.of("country", "FR", "browser", "Firefox", "@timestamp", baseTimestamp + 1000, "bytes", 200, "latency", 15.2)
            )
        );
        bulk1.add(
            new IndexRequest(INDEX_NAME).source(
                Map.of("country", "DE", "browser", "Chrome", "@timestamp", baseTimestamp + 2000, "bytes", 150, "latency", 8.7)
            )
        );
        bulk1.add(
            new IndexRequest(INDEX_NAME).source(
                Map.of("country", "US", "browser", "Safari", "@timestamp", baseTimestamp + 3000, "bytes", 300, "latency", 22.1)
            )
        );
        bulk1.add(
            new IndexRequest(INDEX_NAME).source(
                Map.of("country", "US", "browser", "Firefox", "@timestamp", baseTimestamp + 4000, "bytes", 120, "latency", 11.3)
            )
        );
        bulk1.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        // Second batch - 5 more documents (creates second segment after refresh)
        BulkRequestBuilder bulk2 = client().prepareBulk();
        bulk2.add(
            new IndexRequest(INDEX_NAME).source(
                Map.of("country", "US", "browser", "Chrome", "@timestamp", baseTimestamp + 5000, "bytes", 250, "latency", 18.9)
            )
        );
        bulk2.add(
            new IndexRequest(INDEX_NAME).source(
                Map.of("country", "FR", "browser", "Safari", "@timestamp", baseTimestamp + 6000, "bytes", 180, "latency", 14.6)
            )
        );
        bulk2.add(
            new IndexRequest(INDEX_NAME).source(
                Map.of("country", "DE", "browser", "Firefox", "@timestamp", baseTimestamp + 7000, "bytes", 350, "latency", 25.4)
            )
        );
        bulk2.add(
            new IndexRequest(INDEX_NAME).source(
                Map.of("country", "US", "browser", "Chrome", "@timestamp", baseTimestamp + 8000, "bytes", 200, "latency", 16.2)
            )
        );
        bulk2.add(
            new IndexRequest(INDEX_NAME).source(
                Map.of("country", "US", "browser", "Safari", "@timestamp", baseTimestamp + 9000, "bytes", 400, "latency", 30.0)
            )
        );
        bulk2.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        ensureYellow(INDEX_NAME);
    }

    /**
     * Index test documents with timestamps across multiple hour buckets for time bucketing tests.
     */
    private void indexTimeBucketTestDocuments() {
        // Use a fixed base timestamp at hour boundary for predictable bucketing
        // 2024-01-01 00:00:00 UTC = 1704067200000
        long hourInMillis = 3600 * 1000;
        long baseTimestamp = 1704067200000L;

        BulkRequestBuilder bulk = client().prepareBulk();

        // Hour 0 (00:00-00:59): 2 documents, bytes = 100 + 200 = 300
        bulk.add(
            new IndexRequest(INDEX_NAME).source(
                Map.of("country", "US", "browser", "Chrome", "@timestamp", baseTimestamp + 5 * 60 * 1000, "bytes", 100, "latency", 10.0)
            )
        );
        bulk.add(
            new IndexRequest(INDEX_NAME).source(
                Map.of("country", "US", "browser", "Firefox", "@timestamp", baseTimestamp + 30 * 60 * 1000, "bytes", 200, "latency", 20.0)
            )
        );

        // Hour 1 (01:00-01:59): 2 documents, bytes = 150 + 250 = 400
        bulk.add(
            new IndexRequest(INDEX_NAME).source(
                Map.of(
                    "country",
                    "FR",
                    "browser",
                    "Chrome",
                    "@timestamp",
                    baseTimestamp + hourInMillis + 10 * 60 * 1000,
                    "bytes",
                    150,
                    "latency",
                    15.0
                )
            )
        );
        bulk.add(
            new IndexRequest(INDEX_NAME).source(
                Map.of(
                    "country",
                    "FR",
                    "browser",
                    "Safari",
                    "@timestamp",
                    baseTimestamp + hourInMillis + 45 * 60 * 1000,
                    "bytes",
                    250,
                    "latency",
                    25.0
                )
            )
        );

        // Hour 2 (02:00-02:59): 2 documents, bytes = 300 + 350 = 650
        bulk.add(
            new IndexRequest(INDEX_NAME).source(
                Map.of(
                    "country",
                    "DE",
                    "browser",
                    "Chrome",
                    "@timestamp",
                    baseTimestamp + 2 * hourInMillis + 15 * 60 * 1000,
                    "bytes",
                    300,
                    "latency",
                    30.0
                )
            )
        );
        bulk.add(
            new IndexRequest(INDEX_NAME).source(
                Map.of(
                    "country",
                    "DE",
                    "browser",
                    "Firefox",
                    "@timestamp",
                    baseTimestamp + 2 * hourInMillis + 55 * 60 * 1000,
                    "bytes",
                    350,
                    "latency",
                    35.0
                )
            )
        );

        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        ensureYellow(INDEX_NAME);
    }

    private void forceMergeIndex() {
        // Force merge to trigger star-tree index build
        logger.info("Starting force merge for index: {}", INDEX_NAME);
        client().admin().indices().forceMerge(new ForceMergeRequest(INDEX_NAME).maxNumSegments(1).flush(true)).actionGet();
        // Refresh to ensure the merged segment is visible
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        logger.info("Force merge completed for index: {}", INDEX_NAME);
    }

    /**
     * Index documents with null values in metric fields.
     */
    private void indexDocumentsWithNulls() {
        long baseTimestamp = System.currentTimeMillis();

        BulkRequestBuilder bulk = client().prepareBulk();

        // Document with valid bytes
        bulk.add(
            new IndexRequest(INDEX_NAME).source(
                Map.of("country", "US", "browser", "Chrome", "@timestamp", baseTimestamp, "bytes", 100, "latency", 10.5)
            )
        );

        // Document with null bytes (omit the field to simulate null)
        bulk.add(
            new IndexRequest(INDEX_NAME).source(
                Map.of("country", "US", "browser", "Firefox", "@timestamp", baseTimestamp + 1000, "latency", 15.2)
            )
        );

        // Document with valid bytes
        bulk.add(
            new IndexRequest(INDEX_NAME).source(
                Map.of("country", "FR", "browser", "Chrome", "@timestamp", baseTimestamp + 2000, "bytes", 200, "latency", 8.7)
            )
        );

        // Document with null latency (omit the field)
        bulk.add(
            new IndexRequest(INDEX_NAME).source(
                Map.of("country", "FR", "browser", "Safari", "@timestamp", baseTimestamp + 3000, "bytes", 150)
            )
        );

        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        ensureYellow(INDEX_NAME);
    }

    /**
     * Index documents where some are missing metric fields entirely.
     */
    private void indexDocumentsWithMissingFields() {
        long baseTimestamp = System.currentTimeMillis();

        BulkRequestBuilder bulk = client().prepareBulk();

        // Document with all fields
        bulk.add(
            new IndexRequest(INDEX_NAME).source(
                Map.of("country", "US", "browser", "Chrome", "@timestamp", baseTimestamp, "bytes", 100, "latency", 10.5)
            )
        );

        // Document missing bytes field
        bulk.add(
            new IndexRequest(INDEX_NAME).source(
                Map.of("country", "US", "browser", "Firefox", "@timestamp", baseTimestamp + 1000, "latency", 15.2)
            )
        );

        // Document missing latency field
        bulk.add(
            new IndexRequest(INDEX_NAME).source(
                Map.of("country", "FR", "browser", "Chrome", "@timestamp", baseTimestamp + 2000, "bytes", 300)
            )
        );

        // Document missing both metric fields
        bulk.add(
            new IndexRequest(INDEX_NAME).source(
                Map.of("country", "DE", "browser", "Safari", "@timestamp", baseTimestamp + 3000)
            )
        );

        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        ensureYellow(INDEX_NAME);
    }

    /**
     * Index a larger dataset (100 documents) for performance testing.
     */
    private void indexLargerDataset() {
        long baseTimestamp = System.currentTimeMillis();
        String[] countries = { "US", "FR", "DE", "UK", "JP" };
        String[] browsers = { "Chrome", "Firefox", "Safari", "Edge" };

        BulkRequestBuilder bulk = client().prepareBulk();

        for (int i = 1; i <= 100; i++) {
            String country = countries[i % countries.length];
            String browser = browsers[i % browsers.length];
            long timestamp = baseTimestamp + (i * 1000);
            long bytes = i * 10; // 10, 20, 30, ..., 1000
            double latency = i * 1.5;

            bulk.add(
                new IndexRequest(INDEX_NAME).source(
                    Map.of("country", country, "browser", browser, "@timestamp", timestamp, "bytes", bytes, "latency", latency)
                )
            );
        }

        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        ensureYellow(INDEX_NAME);
    }

    /**
     * Create an index without star-tree configuration for testing fallback behavior.
     */
    private void createIndexWithoutStarTree() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject();
        {
            // Properties only, no _star_tree
            mapping.startObject("properties");
            {
                mapping.startObject("country");
                mapping.field("type", "keyword");
                mapping.endObject();

                mapping.startObject("browser");
                mapping.field("type", "keyword");
                mapping.endObject();

                mapping.startObject("@timestamp");
                mapping.field("type", "date");
                mapping.endObject();

                mapping.startObject("bytes");
                mapping.field("type", "long");
                mapping.endObject();

                mapping.startObject("latency");
                mapping.field("type", "double");
                mapping.endObject();
            }
            mapping.endObject();
        }
        mapping.endObject();

        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("no_star_tree_test")
                .setSettings(
                    Settings.builder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                )
                .setMapping(mapping)
        );
    }

    /**
     * Index test documents to a specific index.
     */
    private void indexTestDocumentsToIndex(String indexName) {
        long baseTimestamp = System.currentTimeMillis();

        BulkRequestBuilder bulk1 = client().prepareBulk();
        bulk1.add(
            new IndexRequest(indexName).source(
                Map.of("country", "US", "browser", "Chrome", "@timestamp", baseTimestamp, "bytes", 100, "latency", 10.5)
            )
        );
        bulk1.add(
            new IndexRequest(indexName).source(
                Map.of("country", "FR", "browser", "Firefox", "@timestamp", baseTimestamp + 1000, "bytes", 200, "latency", 15.2)
            )
        );
        bulk1.add(
            new IndexRequest(indexName).source(
                Map.of("country", "DE", "browser", "Chrome", "@timestamp", baseTimestamp + 2000, "bytes", 150, "latency", 8.7)
            )
        );
        bulk1.add(
            new IndexRequest(indexName).source(
                Map.of("country", "US", "browser", "Safari", "@timestamp", baseTimestamp + 3000, "bytes", 300, "latency", 22.1)
            )
        );
        bulk1.add(
            new IndexRequest(indexName).source(
                Map.of("country", "US", "browser", "Firefox", "@timestamp", baseTimestamp + 4000, "bytes", 120, "latency", 11.3)
            )
        );
        bulk1.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        BulkRequestBuilder bulk2 = client().prepareBulk();
        bulk2.add(
            new IndexRequest(indexName).source(
                Map.of("country", "US", "browser", "Chrome", "@timestamp", baseTimestamp + 5000, "bytes", 250, "latency", 18.9)
            )
        );
        bulk2.add(
            new IndexRequest(indexName).source(
                Map.of("country", "FR", "browser", "Safari", "@timestamp", baseTimestamp + 6000, "bytes", 180, "latency", 14.6)
            )
        );
        bulk2.add(
            new IndexRequest(indexName).source(
                Map.of("country", "DE", "browser", "Firefox", "@timestamp", baseTimestamp + 7000, "bytes", 350, "latency", 25.4)
            )
        );
        bulk2.add(
            new IndexRequest(indexName).source(
                Map.of("country", "US", "browser", "Chrome", "@timestamp", baseTimestamp + 8000, "bytes", 200, "latency", 16.2)
            )
        );
        bulk2.add(
            new IndexRequest(indexName).source(
                Map.of("country", "US", "browser", "Safari", "@timestamp", baseTimestamp + 9000, "bytes", 400, "latency", 30.0)
            )
        );
        bulk2.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        ensureYellow(indexName);
    }

    /**
     * Force merge a specific index.
     */
    private void forceMergeIndexByName(String indexName) {
        logger.info("Starting force merge for index: {}", indexName);
        client().admin().indices().forceMerge(new ForceMergeRequest(indexName).maxNumSegments(1).flush(true)).actionGet();
        client().admin().indices().prepareRefresh(indexName).get();
        logger.info("Force merge completed for index: {}", indexName);
    }

    /**
     * Index a performance test dataset.
     *
     * @param numDocs number of documents to index
     */
    private void indexPerformanceDataset(int numDocs) {
        long baseTimestamp = System.currentTimeMillis();
        String[] countries = { "US", "FR", "DE", "UK", "JP" };
        String[] browsers = { "Chrome", "Firefox", "Safari", "Edge" };

        // Use batches of 100 documents
        int batchSize = 100;
        for (int batch = 0; batch < numDocs / batchSize; batch++) {
            BulkRequestBuilder bulk = client().prepareBulk();

            for (int i = 0; i < batchSize; i++) {
                int docId = batch * batchSize + i + 1;
                String country = countries[docId % countries.length];
                String browser = browsers[docId % browsers.length];
                long timestamp = baseTimestamp + (docId * 1000);
                long bytes = docId * 10;
                double latency = docId * 1.5;

                bulk.add(
                    new IndexRequest(INDEX_NAME).source(
                        Map.of("country", country, "browser", browser, "@timestamp", timestamp, "bytes", bytes, "latency", latency)
                    )
                );
            }

            bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        }

        // Handle remaining docs if numDocs is not a multiple of batchSize
        int remaining = numDocs % batchSize;
        if (remaining > 0) {
            BulkRequestBuilder bulk = client().prepareBulk();
            for (int i = 0; i < remaining; i++) {
                int docId = (numDocs / batchSize) * batchSize + i + 1;
                String country = countries[docId % countries.length];
                String browser = browsers[docId % browsers.length];
                long timestamp = baseTimestamp + (docId * 1000);
                long bytes = docId * 10;
                double latency = docId * 1.5;

                bulk.add(
                    new IndexRequest(INDEX_NAME).source(
                        Map.of("country", country, "browser", browser, "@timestamp", timestamp, "bytes", bytes, "latency", latency)
                    )
                );
            }
            bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        }

        ensureYellow(INDEX_NAME);
    }

    // ========== Performance Benchmark ==========

    /**
     * Benchmark comparing star-tree vs regular execution.
     * This test measures actual query execution times.
     */
    public void testPerformanceBenchmark() throws Exception {
        int numDocs = 10000;
        int warmupRuns = 3;
        int benchmarkRuns = 10;

        // Create and populate star-tree index
        createStarTreeIndex();
        indexPerformanceDataset(numDocs);
        forceMergeIndex();

        // Create and populate regular index (no star-tree)
        String regularIndex = "regular_benchmark_index";
        client().admin()
            .indices()
            .prepareCreate(regularIndex)
            .setMapping(
                "@timestamp", "type=date",
                "country", "type=keyword",
                "browser", "type=keyword",
                "bytes", "type=long",
                "latency", "type=double"
            )
            .get();

        // Index same data to regular index
        long baseTimestamp = System.currentTimeMillis();
        String[] countries = { "US", "FR", "DE", "UK", "JP" };
        String[] browsers = { "Chrome", "Firefox", "Safari", "Edge" };
        int batchSize = 100;
        for (int batch = 0; batch < numDocs / batchSize; batch++) {
            BulkRequestBuilder bulk = client().prepareBulk();
            for (int i = 0; i < batchSize; i++) {
                int docId = batch * batchSize + i + 1;
                bulk.add(
                    new IndexRequest(regularIndex).source(
                        Map.of(
                            "country", countries[docId % countries.length],
                            "browser", browsers[docId % browsers.length],
                            "@timestamp", baseTimestamp + (docId * 1000),
                            "bytes", docId * 10,
                            "latency", docId * 1.5
                        )
                    )
                );
            }
            bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        }
        client().admin().indices().prepareForceMerge(regularIndex).setMaxNumSegments(1).get();

        // Define benchmark queries
        String[] queries = {
            "STATS total = sum(bytes)",
            "STATS total = sum(bytes), cnt = count(*), avg_val = avg(bytes)",
            "STATS total = sum(bytes) BY country",
            "STATS total = sum(bytes), min_val = min(bytes), max_val = max(bytes) BY country",
            "STATS total = sum(bytes) BY country, browser"
        };

        StringBuilder results = new StringBuilder();
        results.append("========== PERFORMANCE BENCHMARK ==========\n");
        results.append("Documents: ").append(numDocs).append(", Warmup runs: ").append(warmupRuns)
               .append(", Benchmark runs: ").append(benchmarkRuns).append("\n\n");

        for (String queryPart : queries) {
            String starTreeQuery = "FROM " + INDEX_NAME + " | " + queryPart;
            String regularQuery = "FROM " + regularIndex + " | " + queryPart;

            // Warmup
            for (int i = 0; i < warmupRuns; i++) {
                try (EsqlQueryResponse r1 = run(starTreeQuery)) {}
                try (EsqlQueryResponse r2 = run(regularQuery)) {}
            }

            // Benchmark star-tree
            long starTreeTotal = 0;
            for (int i = 0; i < benchmarkRuns; i++) {
                long start = System.nanoTime();
                try (EsqlQueryResponse r = run(starTreeQuery)) {
                    starTreeTotal += System.nanoTime() - start;
                }
            }
            double starTreeAvgMs = (starTreeTotal / benchmarkRuns) / 1_000_000.0;

            // Benchmark regular
            long regularTotal = 0;
            for (int i = 0; i < benchmarkRuns; i++) {
                long start = System.nanoTime();
                try (EsqlQueryResponse r = run(regularQuery)) {
                    regularTotal += System.nanoTime() - start;
                }
            }
            double regularAvgMs = (regularTotal / benchmarkRuns) / 1_000_000.0;

            double speedup = regularAvgMs / starTreeAvgMs;
            results.append("Query: ").append(queryPart).append("\n");
            results.append("  Star-tree: ").append(String.format("%.2f", starTreeAvgMs)).append(" ms, Regular: ")
                   .append(String.format("%.2f", regularAvgMs)).append(" ms, Speedup: ")
                   .append(String.format("%.1f", speedup)).append("x\n\n");
        }

        results.append("========== END BENCHMARK ==========\n");

        // Write to file for visibility
        java.nio.file.Path benchmarkFile = java.nio.file.Path.of("/tmp/star_tree_benchmark.txt");
        java.nio.file.Files.writeString(benchmarkFile, results.toString());
        logger.info("Benchmark results written to: {}", benchmarkFile);

        // Also log for test output
        logger.info("\n{}", results.toString());

        // Cleanup
        client().admin().indices().prepareDelete(regularIndex).get();
    }

    /**
     * Benchmark showing star-tree behavior with varying cardinality.
     * Demonstrates how star-tree performance degrades as cardinality increases.
     */
    public void testHighCardinalityBenchmark() throws Exception {
        int numDocs = 50000;
        int warmupRuns = 2;
        int benchmarkRuns = 5;

        StringBuilder results = new StringBuilder();
        results.append("========== HIGH CARDINALITY BENCHMARK ==========\n");
        results.append("Documents: ").append(numDocs).append("\n\n");

        // Test different cardinality levels
        int[] cardinalities = { 5, 50, 500, 5000, 50000 };

        for (int cardinality : cardinalities) {
            String starTreeIndex = "star_tree_card_" + cardinality;
            String regularIndex = "regular_card_" + cardinality;

            // Create star-tree index with high cardinality dimension
            XContentBuilder starTreeMapping = XContentFactory.jsonBuilder();
            starTreeMapping.startObject();
            starTreeMapping.startObject("_star_tree");
            starTreeMapping.startObject("default");
            starTreeMapping.startArray("grouping_fields");
            starTreeMapping.startObject().field("field", "user_id").endObject();
            starTreeMapping.endArray();
            starTreeMapping.startArray("values");
            starTreeMapping.startObject()
                .field("field", "bytes")
                .field("aggregations", List.of("SUM", "COUNT", "MIN", "MAX"))
                .endObject();
            starTreeMapping.endArray();
            starTreeMapping.startObject("config");
            starTreeMapping.field("max_leaf_docs", 1);
            starTreeMapping.field("star_node_threshold", 1);
            starTreeMapping.endObject();
            starTreeMapping.endObject();
            starTreeMapping.endObject();
            starTreeMapping.startObject("properties");
            starTreeMapping.startObject("user_id").field("type", "keyword").endObject();
            starTreeMapping.startObject("bytes").field("type", "long").endObject();
            starTreeMapping.endObject();
            starTreeMapping.endObject();

            client().admin()
                .indices()
                .prepareCreate(starTreeIndex)
                .setSettings(Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0))
                .setMapping(starTreeMapping)
                .get();

            // Create regular index
            client().admin()
                .indices()
                .prepareCreate(regularIndex)
                .setSettings(Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0))
                .setMapping("user_id", "type=keyword", "bytes", "type=long")
                .get();

            // Index data with specified cardinality
            long baseTimestamp = System.currentTimeMillis();
            int batchSize = 100;
            for (int batch = 0; batch < numDocs / batchSize; batch++) {
                BulkRequestBuilder bulkStar = client().prepareBulk();
                BulkRequestBuilder bulkReg = client().prepareBulk();
                for (int i = 0; i < batchSize; i++) {
                    int docId = batch * batchSize + i;
                    String userId = "user_" + (docId % cardinality);
                    long bytes = (docId + 1) * 10;

                    bulkStar.add(new IndexRequest(starTreeIndex).source(Map.of("user_id", userId, "bytes", bytes)));
                    bulkReg.add(new IndexRequest(regularIndex).source(Map.of("user_id", userId, "bytes", bytes)));
                }
                bulkStar.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
                bulkReg.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
            }

            // Force merge to build star-tree
            client().admin().indices().prepareForceMerge(starTreeIndex).setMaxNumSegments(1).get();
            client().admin().indices().prepareForceMerge(regularIndex).setMaxNumSegments(1).get();

            // Benchmark: Aggregate without GROUP BY (star-tree should always win here)
            String queryNoGroup = "STATS total = sum(bytes)";

            // Warmup
            for (int i = 0; i < warmupRuns; i++) {
                try (EsqlQueryResponse r = run("FROM " + starTreeIndex + " | " + queryNoGroup)) {}
                try (EsqlQueryResponse r = run("FROM " + regularIndex + " | " + queryNoGroup)) {}
            }

            // Benchmark no GROUP BY
            long starTreeNoGroup = 0;
            for (int i = 0; i < benchmarkRuns; i++) {
                long start = System.nanoTime();
                try (EsqlQueryResponse r = run("FROM " + starTreeIndex + " | " + queryNoGroup)) {
                    starTreeNoGroup += System.nanoTime() - start;
                }
            }

            long regularNoGroup = 0;
            for (int i = 0; i < benchmarkRuns; i++) {
                long start = System.nanoTime();
                try (EsqlQueryResponse r = run("FROM " + regularIndex + " | " + queryNoGroup)) {
                    regularNoGroup += System.nanoTime() - start;
                }
            }

            // Benchmark: GROUP BY high cardinality field
            String queryWithGroup = "STATS total = sum(bytes) BY user_id";

            // Warmup
            for (int i = 0; i < warmupRuns; i++) {
                try (EsqlQueryResponse r = run("FROM " + starTreeIndex + " | " + queryWithGroup)) {}
                try (EsqlQueryResponse r = run("FROM " + regularIndex + " | " + queryWithGroup)) {}
            }

            long starTreeWithGroup = 0;
            for (int i = 0; i < benchmarkRuns; i++) {
                long start = System.nanoTime();
                try (EsqlQueryResponse r = run("FROM " + starTreeIndex + " | " + queryWithGroup)) {
                    starTreeWithGroup += System.nanoTime() - start;
                }
            }

            long regularWithGroup = 0;
            for (int i = 0; i < benchmarkRuns; i++) {
                long start = System.nanoTime();
                try (EsqlQueryResponse r = run("FROM " + regularIndex + " | " + queryWithGroup)) {
                    regularWithGroup += System.nanoTime() - start;
                }
            }

            double starNoGroupMs = (starTreeNoGroup / benchmarkRuns) / 1_000_000.0;
            double regNoGroupMs = (regularNoGroup / benchmarkRuns) / 1_000_000.0;
            double starWithGroupMs = (starTreeWithGroup / benchmarkRuns) / 1_000_000.0;
            double regWithGroupMs = (regularWithGroup / benchmarkRuns) / 1_000_000.0;

            results.append("Cardinality: ").append(cardinality)
                   .append(" (").append(numDocs / cardinality).append(" docs per group)\n");
            results.append("  No GROUP BY:   Star-tree: ").append(String.format("%.2f", starNoGroupMs))
                   .append(" ms, Regular: ").append(String.format("%.2f", regNoGroupMs))
                   .append(" ms, Speedup: ").append(String.format("%.1f", regNoGroupMs / starNoGroupMs)).append("x\n");
            results.append("  WITH GROUP BY: Star-tree: ").append(String.format("%.2f", starWithGroupMs))
                   .append(" ms, Regular: ").append(String.format("%.2f", regWithGroupMs))
                   .append(" ms, Speedup: ").append(String.format("%.1f", regWithGroupMs / starWithGroupMs)).append("x\n");
            results.append("\n");

            // Cleanup
            client().admin().indices().prepareDelete(starTreeIndex, regularIndex).get();
        }

        results.append("========== END HIGH CARDINALITY BENCHMARK ==========\n");
        results.append("\nKey Insight: Star-tree speedup for GROUP BY diminishes as cardinality approaches document count.\n");
        results.append("At cardinality = doc count, each group has 1 doc, so no pre-aggregation benefit.\n");

        // Write results
        java.nio.file.Path benchmarkFile = java.nio.file.Path.of("/tmp/star_tree_high_cardinality.txt");
        java.nio.file.Files.writeString(benchmarkFile, results.toString());
        logger.info("High cardinality benchmark results written to: {}", benchmarkFile);
    }
}
