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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

/**
 * REST-based integration tests for the upper2() function.
 * <p>
 * These tests verify that the upper2 function works correctly via the REST API,
 * which is the primary way external plugins will be used in production.
 * </p>
 */
public class Upper2RestIT extends ESRestTestCase {

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
     * Test that upper2() function works with a simple literal value.
     * Query: ROW s = "hello" | EVAL u = upper2(s)
     * Expected: u = "HELLO"
     */
    @SuppressWarnings("unchecked")
    public void testUpper2WithLiteral() throws IOException {
        Map<String, Object> result = runQuery("ROW s = \\\"hello\\\" | EVAL u = upper2(s)");

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));

        List<Object> row = values.get(0);
        assertThat(row.get(0), equalTo("hello")); // s
        assertThat(row.get(1), equalTo("HELLO")); // u = upper2(s)
    }

    /**
     * Test upper2() with mixed case input.
     */
    @SuppressWarnings("unchecked")
    public void testUpper2MixedCase() throws IOException {
        Map<String, Object> result = runQuery("ROW s = \\\"HeLLo WoRLd\\\" | EVAL u = upper2(s)");

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));
        assertThat(values.get(0).get(1), equalTo("HELLO WORLD"));
    }

    /**
     * Test upper2() with already uppercase input.
     */
    @SuppressWarnings("unchecked")
    public void testUpper2AlreadyUppercase() throws IOException {
        Map<String, Object> result = runQuery("ROW s = \\\"HELLO\\\" | EVAL u = upper2(s)");

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));
        assertThat(values.get(0).get(1), equalTo("HELLO"));
    }

    /**
     * Test upper2() with empty string.
     */
    @SuppressWarnings("unchecked")
    public void testUpper2EmptyString() throws IOException {
        Map<String, Object> result = runQuery("ROW s = \\\"\\\" | EVAL u = upper2(s)");

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));
        assertThat(values.get(0).get(1), equalTo(""));
    }

    /**
     * Test upper2() with special characters.
     */
    @SuppressWarnings("unchecked")
    public void testUpper2SpecialCharacters() throws IOException {
        Map<String, Object> result = runQuery("ROW s = \\\"test_value-123\\\" | EVAL u = upper2(s)");

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));
        assertThat(values.get(0).get(1), equalTo("TEST_VALUE-123"));
    }

    /**
     * Test upper2() with indexed data.
     */
    @SuppressWarnings("unchecked")
    public void testUpper2WithIndexedData() throws IOException {
        // Create test index
        Request createIndex = new Request("PUT", "/test-upper2");
        createIndex.setJsonEntity("""
            {
              "mappings": {
                "properties": {
                  "name": { "type": "keyword" }
                }
              }
            }
            """);
        client().performRequest(createIndex);

        // Index test documents
        Request indexDoc1 = new Request("POST", "/test-upper2/_doc");
        indexDoc1.setJsonEntity("{\"name\": \"alice\"}");
        client().performRequest(indexDoc1);

        Request indexDoc2 = new Request("POST", "/test-upper2/_doc");
        indexDoc2.setJsonEntity("{\"name\": \"Bob\"}");
        client().performRequest(indexDoc2);

        Request indexDoc3 = new Request("POST", "/test-upper2/_doc");
        indexDoc3.setJsonEntity("{\"name\": \"CHARLIE\"}");
        client().performRequest(indexDoc3);

        // Refresh index
        Request refresh = new Request("POST", "/test-upper2/_refresh");
        client().performRequest(refresh);

        // Query with upper2
        Map<String, Object> result = runQuery("FROM test-upper2 | EVAL upper_name = upper2(name) | SORT name");

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(3));

        // Results sorted by name: Bob, CHARLIE, alice
        assertThat(values.get(0).get(0), equalTo("Bob"));
        assertThat(values.get(0).get(1), equalTo("BOB"));

        assertThat(values.get(1).get(0), equalTo("CHARLIE"));
        assertThat(values.get(1).get(1), equalTo("CHARLIE"));

        assertThat(values.get(2).get(0), equalTo("alice"));
        assertThat(values.get(2).get(1), equalTo("ALICE"));
    }

    /**
     * Test upper2() with null values.
     */
    @SuppressWarnings("unchecked")
    public void testUpper2WithNullValues() throws IOException {
        // Create test index with null values
        Request createIndex = new Request("PUT", "/test-upper2-null");
        createIndex.setJsonEntity("""
            {
              "mappings": {
                "properties": {
                  "name": { "type": "keyword" }
                }
              }
            }
            """);
        client().performRequest(createIndex);

        // Index document with null
        Request indexDoc = new Request("POST", "/test-upper2-null/_doc");
        indexDoc.setJsonEntity("{\"name\": null}");
        client().performRequest(indexDoc);

        // Refresh index
        Request refresh = new Request("POST", "/test-upper2-null/_refresh");
        client().performRequest(refresh);

        // Query with upper2 - null should remain null
        Map<String, Object> result = runQuery("FROM test-upper2-null | EVAL upper_name = upper2(name)");

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));
        assertThat(values.get(0).get(1), nullValue());
    }

    /**
     * Test upper2() combined with other functions.
     */
    @SuppressWarnings("unchecked")
    public void testUpper2CombinedWithOtherFunctions() throws IOException {
        // Test combining upper2 with abs2
        Map<String, Object> result = runQuery("ROW s = \\\"hello\\\", n = -5 | EVAL u = upper2(s), a = abs2(n)");

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));

        List<Object> row = values.get(0);
        assertThat(row.get(0), equalTo("hello")); // s
        assertThat(row.get(1), equalTo(-5));      // n
        assertThat(row.get(2), equalTo("HELLO")); // u = upper2(s)
        assertThat(row.get(3), equalTo(5));       // a = abs2(n)
    }

    /**
     * Test upper2() with large dataset.
     */
    @SuppressWarnings("unchecked")
    public void testUpper2WithLargeDataset() throws IOException {
        // Create test index
        Request createIndex = new Request("PUT", "/test-upper2-large");
        createIndex.setJsonEntity("""
            {
              "mappings": {
                "properties": {
                  "name": { "type": "keyword" }
                }
              }
            }
            """);
        client().performRequest(createIndex);

        // Index 100 documents
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            bulk.append("{\"index\":{}}\n");
            bulk.append("{\"name\": \"test" + i + "\"}\n");
        }

        Request bulkRequest = new Request("POST", "/test-upper2-large/_bulk");
        bulkRequest.setJsonEntity(bulk.toString());
        client().performRequest(bulkRequest);

        // Refresh index
        Request refresh = new Request("POST", "/test-upper2-large/_refresh");
        client().performRequest(refresh);

        // Query with upper2
        Map<String, Object> result = runQuery("FROM test-upper2-large | EVAL upper_name = upper2(name) | LIMIT 10");

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(10));

        // Verify all results are uppercase
        for (List<Object> row : values) {
            String original = (String) row.get(0);
            String upper = (String) row.get(1);
            assertThat(upper, equalTo(original.toUpperCase()));
        }
    }
}
