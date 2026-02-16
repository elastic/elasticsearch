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
 * REST-based integration tests for the add2() binary function.
 * These tests verify that the N-ary runtime evaluator generation works correctly.
 */
public class Add2RestIT extends ESRestTestCase {

    private static final String TEST_INDEX = "test_add2";

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

    /**
     * Test that add2() function works with literal values.
     * Query: ROW a = 3, b = 4 | EVAL sum = add2(a, b)
     * Expected: sum = 7
     */
    @SuppressWarnings("unchecked")
    public void testAdd2WithLiterals() throws IOException {
        Map<String, Object> result = runQuery("ROW a = 3, b = 4 | EVAL sum = add2(a, b)");

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));

        List<Object> row = values.get(0);
        assertThat(row.get(0), equalTo(3)); // a
        assertThat(row.get(1), equalTo(4)); // b
        assertThat(row.get(2), equalTo(7)); // sum = add2(a, b)
    }

    /**
     * Test add2() with negative values.
     */
    @SuppressWarnings("unchecked")
    public void testAdd2WithNegativeValues() throws IOException {
        Map<String, Object> result = runQuery("ROW a = -10, b = 3 | EVAL sum = add2(a, b)");

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));
        assertThat(values.get(0).get(2), equalTo(-7)); // sum = -10 + 3 = -7
    }

    /**
     * Test add2() with double values.
     */
    @SuppressWarnings("unchecked")
    public void testAdd2WithDoubles() throws IOException {
        Map<String, Object> result = runQuery("ROW a = 3.5, b = 2.5 | EVAL sum = add2(a, b)");

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));
        double sum = ((Number) values.get(0).get(2)).doubleValue();
        assertThat(sum, closeTo(6.0, 0.0001));
    }

    /**
     * Test add2() with indexed data.
     */
    @SuppressWarnings("unchecked")
    public void testAdd2WithIndexedData() throws IOException {
        // Index test documents
        indexDoc("1", "{\"a\": 10, \"b\": 5, \"name\": \"x\"}");
        indexDoc("2", "{\"a\": 20, \"b\": 3, \"name\": \"y\"}");
        indexDoc("3", "{\"a\": -5, \"b\": 15, \"name\": \"z\"}");
        refreshIndex();

        // Query with add2
        Map<String, Object> result = runQuery("FROM " + TEST_INDEX + " | EVAL sum = add2(a, b) | KEEP name, a, b, sum | SORT name");

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(3));

        // Row x: a=10, b=5, sum=15
        assertThat(values.get(0).get(0), equalTo("x"));
        assertThat(values.get(0).get(1), equalTo(10));
        assertThat(values.get(0).get(2), equalTo(5));
        assertThat(values.get(0).get(3), equalTo(15));

        // Row y: a=20, b=3, sum=23
        assertThat(values.get(1).get(0), equalTo("y"));
        assertThat(values.get(1).get(1), equalTo(20));
        assertThat(values.get(1).get(2), equalTo(3));
        assertThat(values.get(1).get(3), equalTo(23));

        // Row z: a=-5, b=15, sum=10
        assertThat(values.get(2).get(0), equalTo("z"));
        assertThat(values.get(2).get(1), equalTo(-5));
        assertThat(values.get(2).get(2), equalTo(15));
        assertThat(values.get(2).get(3), equalTo(10));
    }

    /**
     * Test add2() with null values - if either operand is null, result should be null.
     */
    @SuppressWarnings("unchecked")
    public void testAdd2WithNulls() throws IOException {
        // Index documents with nulls
        indexDoc("1", "{\"a\": 10, \"b\": 5, \"name\": \"both\"}");
        indexDoc("2", "{\"a\": null, \"b\": 5, \"name\": \"left_null\"}");
        indexDoc("3", "{\"a\": 10, \"b\": null, \"name\": \"right_null\"}");
        indexDoc("4", "{\"a\": null, \"b\": null, \"name\": \"both_null\"}");
        indexDoc("5", "{\"name\": \"missing\"}"); // Both fields missing = null
        refreshIndex();

        // Query with add2
        Map<String, Object> result = runQuery("FROM " + TEST_INDEX + " | EVAL sum = add2(a, b) | KEEP name, a, b, sum | SORT name");

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(5));

        // Row both: a=10, b=5, sum=15
        assertThat(values.get(0).get(0), equalTo("both"));
        assertThat(values.get(0).get(3), equalTo(15));

        // Row both_null: a=null, b=null, sum=null
        assertThat(values.get(1).get(0), equalTo("both_null"));
        assertThat(values.get(1).get(3), nullValue());

        // Row left_null: a=null, b=5, sum=null
        assertThat(values.get(2).get(0), equalTo("left_null"));
        assertThat(values.get(2).get(3), nullValue());

        // Row missing: a=null, b=null, sum=null
        assertThat(values.get(3).get(0), equalTo("missing"));
        assertThat(values.get(3).get(3), nullValue());

        // Row right_null: a=10, b=null, sum=null
        assertThat(values.get(4).get(0), equalTo("right_null"));
        assertThat(values.get(4).get(3), nullValue());
    }

    /**
     * Test add2() with chained operations.
     */
    @SuppressWarnings("unchecked")
    public void testAdd2Chained() throws IOException {
        Map<String, Object> result = runQuery("ROW a = 1, b = 2, c = 3 | EVAL sum1 = add2(a, b), sum2 = add2(sum1, c)");

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));

        List<Object> row = values.get(0);
        assertThat(row.get(3), equalTo(3));  // sum1 = 1 + 2 = 3
        assertThat(row.get(4), equalTo(6));  // sum2 = 3 + 3 = 6
    }

    /**
     * Test add2() combined with abs2() - verifies both unary and binary work together.
     */
    @SuppressWarnings("unchecked")
    public void testAdd2WithAbs2() throws IOException {
        Map<String, Object> result = runQuery("ROW a = -5, b = 3 | EVAL abs_a = abs2(a), sum = add2(abs_a, b)");

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));

        List<Object> row = values.get(0);
        assertThat(row.get(2), equalTo(5));  // abs_a = abs2(-5) = 5
        assertThat(row.get(3), equalTo(8));  // sum = 5 + 3 = 8
    }

    /**
     * Test add2() with filtering on result.
     */
    @SuppressWarnings("unchecked")
    public void testAdd2WithFiltering() throws IOException {
        // Index test documents
        indexDoc("1", "{\"a\": 10, \"b\": 5, \"name\": \"x\"}");
        indexDoc("2", "{\"a\": 1, \"b\": 2, \"name\": \"y\"}");
        indexDoc("3", "{\"a\": 20, \"b\": 10, \"name\": \"z\"}");
        refreshIndex();

        // Filter where sum > 10
        Map<String, Object> result = runQuery(
            "FROM " + TEST_INDEX + " | EVAL sum = add2(a, b) | WHERE sum > 10 | KEEP name, sum | SORT name"
        );

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(2));

        // Row x: sum=15
        assertThat(values.get(0).get(0), equalTo("x"));
        assertThat(values.get(0).get(1), equalTo(15));

        // Row z: sum=30
        assertThat(values.get(1).get(0), equalTo("z"));
        assertThat(values.get(1).get(1), equalTo(30));
    }

    /**
     * Test add2() with large dataset to verify vector path works.
     */
    @SuppressWarnings("unchecked")
    public void testAdd2WithLargeDataset() throws IOException {
        // Index 50 documents
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < 50; i++) {
            bulk.append("{\"index\": {\"_id\": \"").append(i).append("\"}}\n");
            bulk.append("{\"a\": ").append(i).append(", \"b\": ").append(i * 2).append(", \"name\": \"doc").append(i).append("\"}\n");
        }

        Request bulkRequest = new Request("POST", "/" + TEST_INDEX + "/_bulk");
        bulkRequest.setJsonEntity(bulk.toString());
        bulkRequest.addParameter("refresh", "true");
        client().performRequest(bulkRequest);

        // Count results
        Map<String, Object> result = runQuery("FROM " + TEST_INDEX + " | EVAL sum = add2(a, b) | STATS count = COUNT(*)");

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));
        assertThat(((Number) values.get(0).get(0)).intValue(), equalTo(50));

        // Verify a specific value: doc10 has a=10, b=20, sum=30
        Map<String, Object> detailResult = runQuery("FROM " + TEST_INDEX + " | EVAL sum = add2(a, b) | WHERE a == 10 | KEEP a, b, sum");
        List<List<Object>> detailValues = (List<List<Object>>) detailResult.get("values");
        assertThat(detailValues, hasSize(1));
        assertThat(detailValues.get(0).get(0), equalTo(10));
        assertThat(detailValues.get(0).get(1), equalTo(20));
        assertThat(detailValues.get(0).get(2), equalTo(30));
    }
}
