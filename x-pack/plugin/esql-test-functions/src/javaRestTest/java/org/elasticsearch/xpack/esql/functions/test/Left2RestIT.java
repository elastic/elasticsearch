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
 * REST-based integration tests for the left2() function.
 * <p>
 * These tests verify that the left2 function works correctly via the REST API,
 * testing the @RuntimeFixed parameter support in the runtime evaluator generator.
 * </p>
 */
public class Left2RestIT extends ESRestTestCase {

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
     * Test that left2() function works with a simple literal value.
     * Query: ROW s = "hello world" | EVAL result = left2(s, 5)
     * Expected: result = "hello"
     */
    @SuppressWarnings("unchecked")
    public void testLeft2Basic() throws IOException {
        Map<String, Object> result = runQuery("ROW s = \\\"hello world\\\" | EVAL result = left2(s, 5)");

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));

        List<Object> row = values.get(0);
        assertThat(row.get(0), equalTo("hello world")); // s
        assertThat(row.get(1), equalTo("hello"));       // result = left2(s, 5)
    }

    /**
     * Test left2() with length longer than string.
     */
    @SuppressWarnings("unchecked")
    public void testLeft2LongerThanString() throws IOException {
        Map<String, Object> result = runQuery("ROW s = \\\"hi\\\" | EVAL result = left2(s, 10)");

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));

        List<Object> row = values.get(0);
        assertThat(row.get(1), equalTo("hi")); // Should return entire string
    }

    /**
     * Test left2() with length of zero.
     */
    @SuppressWarnings("unchecked")
    public void testLeft2ZeroLength() throws IOException {
        Map<String, Object> result = runQuery("ROW s = \\\"hello\\\" | EVAL result = left2(s, 0)");

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));

        List<Object> row = values.get(0);
        assertThat(row.get(1), equalTo("")); // Should return empty string
    }

    /**
     * Test left2() with length of one.
     */
    @SuppressWarnings("unchecked")
    public void testLeft2OneChar() throws IOException {
        Map<String, Object> result = runQuery("ROW s = \\\"hello\\\" | EVAL result = left2(s, 1)");

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));

        List<Object> row = values.get(0);
        assertThat(row.get(1), equalTo("h")); // Should return first character
    }

    /**
     * Test left2() with indexed data.
     */
    @SuppressWarnings("unchecked")
    public void testLeft2WithIndexedData() throws IOException {
        // Create test index
        Request createIndex = new Request("PUT", "/test-left2");
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
        Request indexDoc1 = new Request("POST", "/test-left2/_doc");
        indexDoc1.setJsonEntity("{\"name\": \"elasticsearch\"}");
        client().performRequest(indexDoc1);

        Request indexDoc2 = new Request("POST", "/test-left2/_doc");
        indexDoc2.setJsonEntity("{\"name\": \"kibana\"}");
        client().performRequest(indexDoc2);

        Request indexDoc3 = new Request("POST", "/test-left2/_doc");
        indexDoc3.setJsonEntity("{\"name\": \"logstash\"}");
        client().performRequest(indexDoc3);

        // Refresh index
        Request refresh = new Request("POST", "/test-left2/_refresh");
        client().performRequest(refresh);

        // Query with left2
        Map<String, Object> result = runQuery("FROM test-left2 | EVAL short_name = left2(name, 3) | SORT name");

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(3));

        // Results sorted by name: elasticsearch, kibana, logstash
        assertThat(values.get(0).get(0), equalTo("elasticsearch"));
        assertThat(values.get(0).get(1), equalTo("ela"));

        assertThat(values.get(1).get(0), equalTo("kibana"));
        assertThat(values.get(1).get(1), equalTo("kib"));

        assertThat(values.get(2).get(0), equalTo("logstash"));
        assertThat(values.get(2).get(1), equalTo("log"));
    }

    /**
     * Test left2() with null values.
     */
    @SuppressWarnings("unchecked")
    public void testLeft2WithNullValues() throws IOException {
        // Create test index with null values
        Request createIndex = new Request("PUT", "/test-left2-null");
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
        Request indexDoc = new Request("POST", "/test-left2-null/_doc");
        indexDoc.setJsonEntity("{\"name\": null}");
        client().performRequest(indexDoc);

        // Refresh index
        Request refresh = new Request("POST", "/test-left2-null/_refresh");
        client().performRequest(refresh);

        // Query with left2 - null should remain null
        Map<String, Object> result = runQuery("FROM test-left2-null | EVAL short_name = left2(name, 5)");

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));
        assertThat(values.get(0).get(1), nullValue());
    }

    /**
     * Test left2() produces same results as built-in LEFT() for various inputs.
     */
    @SuppressWarnings("unchecked")
    public void testLeft2MatchesLeft() throws IOException {
        Map<String, Object> result = runQuery("ROW s = \\\"hello world\\\" | EVAL left_result = LEFT(s, 5), left2_result = left2(s, 5)");

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));

        List<Object> row = values.get(0);
        String leftResult = (String) row.get(1);
        String left2Result = (String) row.get(2);

        assertThat(leftResult, equalTo("hello"));
        assertThat(left2Result, equalTo("hello"));
        assertThat(left2Result, equalTo(leftResult));
    }
}
