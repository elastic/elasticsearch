/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.functions.test;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Integration tests for the Greatest2 variadic function.
 * <p>
 * These tests verify that the runtime-generated variadic evaluator works correctly
 * for functions that take a variable number of arguments.
 */
public class Greatest2RestIT extends ESRestTestCase {

    private static final String TEST_INDEX = "test_greatest2";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Before
    public void setupIndex() throws IOException {
        // Create test index
        Request createIndex = new Request("PUT", "/" + TEST_INDEX);
        createIndex.setJsonEntity("""
            {
                "mappings": {
                    "properties": {
                        "a": { "type": "integer" },
                        "b": { "type": "integer" },
                        "c": { "type": "integer" }
                    }
                }
            }
            """);
        client().performRequest(createIndex);
    }

    @After
    public void cleanupIndex() throws IOException {
        try {
            client().performRequest(new Request("DELETE", "/" + TEST_INDEX));
        } catch (Exception e) {
            // Ignore if index doesn't exist
        }
    }

    // ==================== Helper Methods ====================

    private Map<String, Object> runQuery(String query) throws IOException {
        Request request = new Request("POST", "/_query");
        request.setJsonEntity("{\"query\": \"" + query + "\"}");
        // Ignore warnings about default limit
        request.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE).build());
        Response response = client().performRequest(request);
        String responseBody = EntityUtils.toString(response.getEntity());
        return org.elasticsearch.xcontent.json.JsonXContent.jsonXContent.createParser(
            org.elasticsearch.xcontent.XContentParserConfiguration.EMPTY,
            responseBody
        ).map();
    }

    // ==================== Tests ====================

    public void testGreatest2WithTwoArguments() throws IOException {
        Map<String, Object> result = runQuery("ROW a = 3, b = 7 | EVAL max = greatest2(a, b) | KEEP max");

        @SuppressWarnings("unchecked")
        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));
        assertThat(((Number) values.get(0).get(0)).intValue(), equalTo(7));
    }

    public void testGreatest2WithThreeArguments() throws IOException {
        Map<String, Object> result = runQuery("ROW a = 3, b = 7, c = 5 | EVAL max = greatest2(a, b, c) | KEEP max");

        @SuppressWarnings("unchecked")
        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));
        assertThat(((Number) values.get(0).get(0)).intValue(), equalTo(7));
    }

    public void testGreatest2WithFiveArguments() throws IOException {
        Map<String, Object> result = runQuery("ROW a = 1, b = 9, c = 3, d = 7, e = 5 | EVAL max = greatest2(a, b, c, d, e) | KEEP max");

        @SuppressWarnings("unchecked")
        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));
        assertThat(((Number) values.get(0).get(0)).intValue(), equalTo(9));
    }

    public void testGreatest2WithNegativeNumbers() throws IOException {
        Map<String, Object> result = runQuery("ROW a = -5, b = -2, c = -8 | EVAL max = greatest2(a, b, c) | KEEP max");

        @SuppressWarnings("unchecked")
        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));
        assertThat(((Number) values.get(0).get(0)).intValue(), equalTo(-2));
    }

    public void testGreatest2WithSameValues() throws IOException {
        Map<String, Object> result = runQuery("ROW a = 5, b = 5, c = 5 | EVAL max = greatest2(a, b, c) | KEEP max");

        @SuppressWarnings("unchecked")
        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));
        assertThat(((Number) values.get(0).get(0)).intValue(), equalTo(5));
    }

    public void testGreatest2MatchesBuiltIn() throws IOException {
        // Compare with built-in GREATEST function
        Map<String, Object> result = runQuery(
            "ROW a = 3, b = 7, c = 5 | EVAL max2 = greatest2(a, b, c), max_builtin = GREATEST(a, b, c) | KEEP max2, max_builtin"
        );

        @SuppressWarnings("unchecked")
        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));
        int max2 = ((Number) values.get(0).get(0)).intValue();
        int maxBuiltin = ((Number) values.get(0).get(1)).intValue();
        assertThat(max2, equalTo(maxBuiltin));
        assertThat(max2, equalTo(7));
    }

    public void testGreatest2WithIndexedData() throws IOException {
        // Index some test data
        StringBuilder bulk = new StringBuilder();
        int[][] testData = { { 1, 5, 3 }, { 10, 2, 8 }, { 4, 4, 4 }, { -1, -5, -3 } };

        for (int i = 0; i < testData.length; i++) {
            bulk.append("{\"index\": {\"_id\": \"").append(i).append("\"}}\n");
            bulk.append("{\"a\": ")
                .append(testData[i][0])
                .append(", \"b\": ")
                .append(testData[i][1])
                .append(", \"c\": ")
                .append(testData[i][2])
                .append("}\n");
        }

        Request bulkRequest = new Request("POST", "/" + TEST_INDEX + "/_bulk");
        bulkRequest.setJsonEntity(bulk.toString());
        bulkRequest.addParameter("refresh", "true");
        client().performRequest(bulkRequest);

        // Query with greatest2
        Map<String, Object> result = runQuery(
            "FROM " + TEST_INDEX + " | EVAL max = greatest2(a, b, c) | SORT max DESC | KEEP a, b, c, max"
        );

        @SuppressWarnings("unchecked")
        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(4));

        // First row should have max = 10 (from 10, 2, 8)
        assertThat(((Number) values.get(0).get(3)).intValue(), equalTo(10));

        // Second row should have max = 5 (from 1, 5, 3)
        assertThat(((Number) values.get(1).get(3)).intValue(), equalTo(5));

        // Third row should have max = 4 (from 4, 4, 4)
        assertThat(((Number) values.get(2).get(3)).intValue(), equalTo(4));

        // Fourth row should have max = -1 (from -1, -5, -3)
        assertThat(((Number) values.get(3).get(3)).intValue(), equalTo(-1));
    }

    public void testGreatest2WithNullValues() throws IOException {
        // Index data with null values - use different 'a' values for deterministic sorting
        StringBuilder bulk = new StringBuilder();
        bulk.append("{\"index\": {\"_id\": \"1\"}}\n");
        bulk.append("{\"a\": 1, \"b\": 3, \"c\": 7}\n");
        bulk.append("{\"index\": {\"_id\": \"2\"}}\n");
        bulk.append("{\"a\": 2, \"b\": null, \"c\": 7}\n"); // b is null
        bulk.append("{\"index\": {\"_id\": \"3\"}}\n");
        bulk.append("{\"a\": 3, \"b\": 3}\n"); // c is missing (null)

        Request bulkRequest = new Request("POST", "/" + TEST_INDEX + "/_bulk");
        bulkRequest.setJsonEntity(bulk.toString());
        bulkRequest.addParameter("refresh", "true");
        client().performRequest(bulkRequest);

        // Query - rows with null should produce null result
        Map<String, Object> result = runQuery("FROM " + TEST_INDEX + " | EVAL max = greatest2(a, b, c) | SORT a | KEEP a, b, c, max");

        @SuppressWarnings("unchecked")
        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(3));

        // First row (1, 3, 7) should have max = 7
        assertThat(((Number) values.get(0).get(3)).intValue(), equalTo(7));

        // Second row (2, null, 7) should have max = null
        assertThat(values.get(1).get(3), equalTo(null));

        // Third row (3, 3, null) should have max = null
        assertThat(values.get(2).get(3), equalTo(null));
    }

    public void testGreatest2WithLongType() throws IOException {
        Map<String, Object> result = runQuery(
            "ROW a = 1000000000000::long, b = 2000000000000::long, c = 1500000000000::long | EVAL max = greatest2(a, b, c) | KEEP max"
        );

        @SuppressWarnings("unchecked")
        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));
        assertThat(((Number) values.get(0).get(0)).longValue(), equalTo(2000000000000L));
    }

    public void testGreatest2WithDoubleType() throws IOException {
        Map<String, Object> result = runQuery("ROW a = 1.5, b = 2.7, c = 2.3 | EVAL max = greatest2(a, b, c) | KEEP max");

        @SuppressWarnings("unchecked")
        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));
        assertThat(((Number) values.get(0).get(0)).doubleValue(), equalTo(2.7));
    }
}
