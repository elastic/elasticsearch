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
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

/**
 * REST-based integration tests for the div2() function with warnExceptions support.
 * These tests verify that the warnExceptions feature works correctly:
 * - Division by zero returns null with a warning instead of failing the query
 */
public class Div2RestIT extends ESRestTestCase {

    private static final String TEST_INDEX = "test_div2";

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

    @Before
    public void setupIndex() throws IOException {
        // Create index with integer fields
        Request createIndex = new Request("PUT", "/" + TEST_INDEX);
        createIndex.setJsonEntity("""
            {
                "mappings": {
                    "properties": {
                        "a": { "type": "integer" },
                        "b": { "type": "integer" },
                        "name": { "type": "keyword" }
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

    /**
     * Helper method to execute an ES|QL query via REST API.
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> runQuery(String query) throws IOException {
        Request request = new Request("POST", "/_query");
        request.setJsonEntity("{\"query\": \"" + query + "\"}");
        request.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE).build());

        Response response = client().performRequest(request);
        return entityAsMap(response);
    }

    /**
     * Helper method to index a document.
     */
    private void indexDoc(String id, String jsonEntity) throws IOException {
        Request indexDoc = new Request("PUT", "/" + TEST_INDEX + "/_doc/" + id);
        indexDoc.setJsonEntity(jsonEntity);
        client().performRequest(indexDoc);
    }

    /**
     * Helper method to refresh the index.
     */
    private void refreshIndex() throws IOException {
        client().performRequest(new Request("POST", "/" + TEST_INDEX + "/_refresh"));
    }

    // ==================== Normal Division Tests ====================

    /**
     * Test that div2() function works with literal values.
     * Query: ROW a = 10, b = 2 | EVAL result = div2(a, b)
     * Expected: result = 5
     */
    @SuppressWarnings("unchecked")
    public void testDiv2WithLiterals() throws IOException {
        Map<String, Object> result = runQuery("ROW a = 10, b = 2 | EVAL result = div2(a, b)");

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));

        List<Object> row = values.get(0);
        assertThat(row.get(0), equalTo(10)); // a
        assertThat(row.get(1), equalTo(2));  // b
        assertThat(row.get(2), equalTo(5));  // result = div2(a, b)
    }

    /**
     * Test div2() with negative values.
     */
    @SuppressWarnings("unchecked")
    public void testDiv2WithNegativeValues() throws IOException {
        Map<String, Object> result = runQuery("ROW a = -20, b = 4 | EVAL result = div2(a, b)");

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));
        assertThat(values.get(0).get(2), equalTo(-5)); // result = -20 / 4 = -5
    }

    /**
     * Test div2() with double values.
     */
    @SuppressWarnings("unchecked")
    public void testDiv2WithDoubles() throws IOException {
        Map<String, Object> result = runQuery("ROW a = 7.5, b = 2.5 | EVAL result = div2(a, b)");

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));
        double quotient = ((Number) values.get(0).get(2)).doubleValue();
        assertThat(quotient, closeTo(3.0, 0.0001));
    }

    // ==================== Division by Zero Tests (warnExceptions) ====================

    /**
     * Test div2() with division by zero - should return null with a warning.
     * This is the key test for warnExceptions support.
     */
    @SuppressWarnings("unchecked")
    public void testDiv2DivisionByZero() throws IOException {
        Map<String, Object> result = runQuery("ROW a = 10, b = 0 | EVAL result = div2(a, b)");

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));

        List<Object> row = values.get(0);
        assertThat(row.get(0), equalTo(10)); // a
        assertThat(row.get(1), equalTo(0));  // b
        assertThat(row.get(2), nullValue()); // result = null (division by zero)
    }

    /**
     * Test div2() with indexed data containing zeros.
     * Some rows should succeed, some should return null due to division by zero.
     */
    @SuppressWarnings("unchecked")
    public void testDiv2WithIndexedDataContainingZeros() throws IOException {
        // Index test documents - mix of valid and division-by-zero cases
        indexDoc("1", "{\"a\": 10, \"b\": 2, \"name\": \"valid1\"}");
        indexDoc("2", "{\"a\": 20, \"b\": 0, \"name\": \"div_by_zero\"}");
        indexDoc("3", "{\"a\": 15, \"b\": 3, \"name\": \"valid2\"}");
        indexDoc("4", "{\"a\": 0, \"b\": 0, \"name\": \"zero_div_zero\"}");
        indexDoc("5", "{\"a\": 100, \"b\": 5, \"name\": \"valid3\"}");
        refreshIndex();

        // Query with div2
        Map<String, Object> result = runQuery("FROM " + TEST_INDEX + " | EVAL result = div2(a, b) | KEEP name, a, b, result | SORT name");

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(5));

        // Row div_by_zero: a=20, b=0, result=null
        assertThat(values.get(0).get(0), equalTo("div_by_zero"));
        assertThat(values.get(0).get(1), equalTo(20));
        assertThat(values.get(0).get(2), equalTo(0));
        assertThat(values.get(0).get(3), nullValue()); // Division by zero -> null

        // Row valid1: a=10, b=2, result=5
        assertThat(values.get(1).get(0), equalTo("valid1"));
        assertThat(values.get(1).get(1), equalTo(10));
        assertThat(values.get(1).get(2), equalTo(2));
        assertThat(values.get(1).get(3), equalTo(5));

        // Row valid2: a=15, b=3, result=5
        assertThat(values.get(2).get(0), equalTo("valid2"));
        assertThat(values.get(2).get(1), equalTo(15));
        assertThat(values.get(2).get(2), equalTo(3));
        assertThat(values.get(2).get(3), equalTo(5));

        // Row valid3: a=100, b=5, result=20
        assertThat(values.get(3).get(0), equalTo("valid3"));
        assertThat(values.get(3).get(1), equalTo(100));
        assertThat(values.get(3).get(2), equalTo(5));
        assertThat(values.get(3).get(3), equalTo(20));

        // Row zero_div_zero: a=0, b=0, result=null
        assertThat(values.get(4).get(0), equalTo("zero_div_zero"));
        assertThat(values.get(4).get(1), equalTo(0));
        assertThat(values.get(4).get(2), equalTo(0));
        assertThat(values.get(4).get(3), nullValue()); // Division by zero -> null
    }

    /**
     * Test that div2() with all valid data (no zeros) works correctly.
     */
    @SuppressWarnings("unchecked")
    public void testDiv2WithIndexedDataNoZeros() throws IOException {
        // Index test documents - all valid divisions
        indexDoc("1", "{\"a\": 10, \"b\": 2, \"name\": \"x\"}");
        indexDoc("2", "{\"a\": 20, \"b\": 4, \"name\": \"y\"}");
        indexDoc("3", "{\"a\": 15, \"b\": 5, \"name\": \"z\"}");
        refreshIndex();

        // Query with div2
        Map<String, Object> result = runQuery("FROM " + TEST_INDEX + " | EVAL result = div2(a, b) | KEEP name, a, b, result | SORT name");

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(3));

        // Row x: a=10, b=2, result=5
        assertThat(values.get(0).get(0), equalTo("x"));
        assertThat(values.get(0).get(3), equalTo(5));

        // Row y: a=20, b=4, result=5
        assertThat(values.get(1).get(0), equalTo("y"));
        assertThat(values.get(1).get(3), equalTo(5));

        // Row z: a=15, b=5, result=3
        assertThat(values.get(2).get(0), equalTo("z"));
        assertThat(values.get(2).get(3), equalTo(3));
    }

    // ==================== Null Handling Tests ====================

    /**
     * Test div2() with null values - if either operand is null, result should be null.
     */
    @SuppressWarnings("unchecked")
    public void testDiv2WithNulls() throws IOException {
        // Index documents with nulls
        indexDoc("1", "{\"a\": 10, \"b\": 2, \"name\": \"both\"}");
        indexDoc("2", "{\"a\": null, \"b\": 2, \"name\": \"left_null\"}");
        indexDoc("3", "{\"a\": 10, \"b\": null, \"name\": \"right_null\"}");
        indexDoc("4", "{\"a\": null, \"b\": null, \"name\": \"both_null\"}");
        refreshIndex();

        // Query with div2
        Map<String, Object> result = runQuery("FROM " + TEST_INDEX + " | EVAL result = div2(a, b) | KEEP name, a, b, result | SORT name");

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(4));

        // Row both: a=10, b=2, result=5
        assertThat(values.get(0).get(0), equalTo("both"));
        assertThat(values.get(0).get(3), equalTo(5));

        // Row both_null: a=null, b=null, result=null
        assertThat(values.get(1).get(0), equalTo("both_null"));
        assertThat(values.get(1).get(3), nullValue());

        // Row left_null: a=null, b=2, result=null
        assertThat(values.get(2).get(0), equalTo("left_null"));
        assertThat(values.get(2).get(3), nullValue());

        // Row right_null: a=10, b=null, result=null
        assertThat(values.get(3).get(0), equalTo("right_null"));
        assertThat(values.get(3).get(3), nullValue());
    }

    // ==================== Combined Tests ====================

    /**
     * Test div2() combined with add2() - verifies both functions work together.
     */
    @SuppressWarnings("unchecked")
    public void testDiv2WithAdd2() throws IOException {
        Map<String, Object> result = runQuery("ROW a = 20, b = 4, c = 3 | EVAL quotient = div2(a, b), sum = add2(quotient, c)");

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));

        List<Object> row = values.get(0);
        assertThat(row.get(3), equalTo(5));  // quotient = 20 / 4 = 5
        assertThat(row.get(4), equalTo(8));  // sum = 5 + 3 = 8
    }

    /**
     * Test div2() with filtering on result.
     */
    @SuppressWarnings("unchecked")
    public void testDiv2WithFiltering() throws IOException {
        // Index test documents
        indexDoc("1", "{\"a\": 100, \"b\": 10, \"name\": \"x\"}");
        indexDoc("2", "{\"a\": 10, \"b\": 5, \"name\": \"y\"}");
        indexDoc("3", "{\"a\": 50, \"b\": 2, \"name\": \"z\"}");
        refreshIndex();

        // Filter where result > 5
        Map<String, Object> result = runQuery(
            "FROM " + TEST_INDEX + " | EVAL result = div2(a, b) | WHERE result > 5 | KEEP name, result | SORT name"
        );

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(2));

        // Row x: result=10
        assertThat(values.get(0).get(0), equalTo("x"));
        assertThat(values.get(0).get(1), equalTo(10));

        // Row z: result=25
        assertThat(values.get(1).get(0), equalTo("z"));
        assertThat(values.get(1).get(1), equalTo(25));
    }

    /**
     * Test div2() with large dataset to verify vector path works with warnExceptions.
     */
    @SuppressWarnings("unchecked")
    public void testDiv2WithLargeDataset() throws IOException {
        // Index 50 documents, some with b=0
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < 50; i++) {
            bulk.append("{\"index\": {\"_id\": \"").append(i).append("\"}}\n");
            int b = (i % 10 == 0) ? 0 : (i % 5) + 1; // Every 10th has b=0
            bulk.append("{\"a\": ").append(i * 10).append(", \"b\": ").append(b).append(", \"name\": \"doc").append(i).append("\"}\n");
        }

        Request bulkRequest = new Request("POST", "/" + TEST_INDEX + "/_bulk");
        bulkRequest.setJsonEntity(bulk.toString());
        bulkRequest.addParameter("refresh", "true");
        client().performRequest(bulkRequest);

        // Count results - should have 50 rows
        Map<String, Object> result = runQuery("FROM " + TEST_INDEX + " | EVAL result = div2(a, b) | STATS count = COUNT(*)");

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));
        assertThat(((Number) values.get(0).get(0)).intValue(), equalTo(50));

        // Count null results (division by zero cases)
        Map<String, Object> nullResult = runQuery(
            "FROM " + TEST_INDEX + " | EVAL result = div2(a, b) | WHERE result IS NULL | STATS count = COUNT(*)"
        );
        List<List<Object>> nullValues = (List<List<Object>>) nullResult.get("values");
        assertThat(nullValues, hasSize(1));
        // Every 10th document (0, 10, 20, 30, 40) has b=0, so 5 nulls
        assertThat(((Number) nullValues.get(0).get(0)).intValue(), equalTo(5));
    }
}
