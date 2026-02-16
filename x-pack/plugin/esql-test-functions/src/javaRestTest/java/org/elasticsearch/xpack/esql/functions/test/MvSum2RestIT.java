/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.functions.test;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

/**
 * REST-based integration tests for the mv_sum2() function.
 * <p>
 * These tests verify that the mv_sum2 function works correctly via the REST API.
 * mv_sum2 is an MvEvaluator that reduces multivalued fields to a single value
 * by summing all values.
 * </p>
 */
public class MvSum2RestIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.enabled", "false")
        .setting("xpack.security.http.ssl.enabled", "false")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    /**
     * Helper method to execute an ES|QL query via REST API.
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> runQuery(String query) throws IOException {
        Request request = new Request("POST", "/_query");
        request.setJsonEntity("{\"query\": \"" + query + "\"}");
        // Allow warnings (e.g., "No limit defined, adding default limit of [1000]")
        request.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE).build());

        Response response = client().performRequest(request);
        return entityAsMap(response);
    }

    /**
     * Test that mv_sum2() function works with a simple multivalued literal.
     * Query: ROW x = [1, 2, 3] | EVAL sum = mv_sum2(x)
     * Expected: sum = 6
     */
    @SuppressWarnings("unchecked")
    public void testMvSum2WithMultivaluedLiteral() throws IOException {
        Map<String, Object> result = runQuery("ROW x = [1, 2, 3] | EVAL sum = mv_sum2(x)");

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));

        List<Object> row = values.get(0);
        // x is multivalued [1, 2, 3]
        // sum = 1 + 2 + 3 = 6
        assertThat(row.get(1), equalTo(6));
    }

    /**
     * Test that mv_sum2() produces the same results as mv_sum() for various inputs.
     */
    @SuppressWarnings("unchecked")
    public void testMvSum2MatchesMvSum() throws IOException {
        Map<String, Object> result = runQuery("ROW x = [10, 20, 30, 40] | EVAL mv_sum_result = mv_sum(x), mv_sum2_result = mv_sum2(x)");

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));

        List<Object> row = values.get(0);
        int mvSumResult = ((Number) row.get(1)).intValue();
        int mvSum2Result = ((Number) row.get(2)).intValue();

        assertThat(mvSumResult, equalTo(100));
        assertThat(mvSum2Result, equalTo(100));
        assertThat(mvSum2Result, equalTo(mvSumResult));
    }

    /**
     * Test mv_sum2() with single-valued field (should return the value unchanged).
     */
    @SuppressWarnings("unchecked")
    public void testMvSum2WithSingleValue() throws IOException {
        Map<String, Object> result = runQuery("ROW x = 42 | EVAL sum = mv_sum2(x)");

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));

        List<Object> row = values.get(0);
        assertThat(row.get(0), equalTo(42)); // x
        assertThat(row.get(1), equalTo(42)); // sum = mv_sum2(x) for single value
    }

    /**
     * Test mv_sum2() with double values.
     */
    @SuppressWarnings("unchecked")
    public void testMvSum2WithDoubles() throws IOException {
        Map<String, Object> result = runQuery("ROW x = [1.5, 2.5, 3.0] | EVAL sum = mv_sum2(x)");

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));

        List<Object> row = values.get(0);
        double sum = ((Number) row.get(1)).doubleValue();
        assertThat(sum, closeTo(7.0, 0.0001));
    }

    /**
     * Test mv_sum2() with long values.
     */
    @SuppressWarnings("unchecked")
    public void testMvSum2WithLongs() throws IOException {
        Map<String, Object> result = runQuery("ROW x = [1000000000000, 2000000000000, 3000000000000] | EVAL sum = mv_sum2(x)");

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));

        List<Object> row = values.get(0);
        long sum = ((Number) row.get(1)).longValue();
        assertThat(sum, equalTo(6000000000000L));
    }

    /**
     * Test mv_sum2() with indexed multivalued data.
     */
    @SuppressWarnings("unchecked")
    public void testMvSum2WithIndexedData() throws IOException {
        // Create test index with multivalued field
        Request createIndex = new Request("PUT", "/test-mvsum2");
        createIndex.setJsonEntity("""
            {
              "mappings": {
                "properties": {
                  "values": { "type": "integer" },
                  "id": { "type": "integer" }
                }
              }
            }
            """);
        client().performRequest(createIndex);

        // Index test documents with multivalued fields
        Request indexDoc1 = new Request("POST", "/test-mvsum2/_doc");
        indexDoc1.setJsonEntity("{\"id\": 1, \"values\": [1, 2, 3]}");
        client().performRequest(indexDoc1);

        Request indexDoc2 = new Request("POST", "/test-mvsum2/_doc");
        indexDoc2.setJsonEntity("{\"id\": 2, \"values\": [10, 20]}");
        client().performRequest(indexDoc2);

        Request indexDoc3 = new Request("POST", "/test-mvsum2/_doc");
        indexDoc3.setJsonEntity("{\"id\": 3, \"values\": [100]}");
        client().performRequest(indexDoc3);

        // Refresh index
        Request refresh = new Request("POST", "/test-mvsum2/_refresh");
        client().performRequest(refresh);

        // Query with mv_sum2
        Map<String, Object> result = runQuery("FROM test-mvsum2 | EVAL sum = mv_sum2(values) | SORT id");

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(3));

        // Document 1: [1, 2, 3] -> sum = 6
        assertThat(values.get(0).get(0), equalTo(1)); // id
        assertThat(values.get(0).get(2), equalTo(6)); // sum

        // Document 2: [10, 20] -> sum = 30
        assertThat(values.get(1).get(0), equalTo(2)); // id
        assertThat(values.get(1).get(2), equalTo(30)); // sum

        // Document 3: [100] -> sum = 100
        assertThat(values.get(2).get(0), equalTo(3)); // id
        assertThat(values.get(2).get(2), equalTo(100)); // sum
    }

    /**
     * Test mv_sum2() with null/empty multivalued fields.
     */
    @SuppressWarnings("unchecked")
    public void testMvSum2WithNullValues() throws IOException {
        // Create test index
        Request createIndex = new Request("PUT", "/test-mvsum2-null");
        createIndex.setJsonEntity("""
            {
              "mappings": {
                "properties": {
                  "values": { "type": "integer" },
                  "id": { "type": "integer" }
                }
              }
            }
            """);
        client().performRequest(createIndex);

        // Index document with null values field
        Request indexDoc = new Request("POST", "/test-mvsum2-null/_doc");
        indexDoc.setJsonEntity("{\"id\": 1, \"values\": null}");
        client().performRequest(indexDoc);

        // Refresh index
        Request refresh = new Request("POST", "/test-mvsum2-null/_refresh");
        client().performRequest(refresh);

        // Query with mv_sum2 - null should remain null
        Map<String, Object> result = runQuery("FROM test-mvsum2-null | EVAL sum = mv_sum2(values)");

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));
        assertThat(values.get(0).get(2), nullValue()); // sum should be null
    }

    /**
     * Test mv_sum2() with negative values.
     */
    @SuppressWarnings("unchecked")
    public void testMvSum2WithNegativeValues() throws IOException {
        Map<String, Object> result = runQuery("ROW x = [-5, 10, -3, 8] | EVAL sum = mv_sum2(x)");

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));

        List<Object> row = values.get(0);
        // sum = -5 + 10 + (-3) + 8 = 10
        assertThat(row.get(1), equalTo(10));
    }

    /**
     * Test mv_sum2() with two values (minimum for pairwise processing).
     */
    @SuppressWarnings("unchecked")
    public void testMvSum2WithTwoValues() throws IOException {
        Map<String, Object> result = runQuery("ROW x = [7, 3] | EVAL sum = mv_sum2(x)");

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));

        List<Object> row = values.get(0);
        assertThat(row.get(1), equalTo(10)); // 7 + 3 = 10
    }

    /**
     * Test mv_sum2() with many values.
     */
    @SuppressWarnings("unchecked")
    public void testMvSum2WithManyValues() throws IOException {
        Map<String, Object> result = runQuery("ROW x = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10] | EVAL sum = mv_sum2(x)");

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));

        List<Object> row = values.get(0);
        // sum = 1 + 2 + ... + 10 = 55
        assertThat(row.get(1), equalTo(55));
    }
}
