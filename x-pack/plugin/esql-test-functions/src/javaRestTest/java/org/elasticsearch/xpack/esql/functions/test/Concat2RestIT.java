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
 * Integration tests for the Concat2 variadic string concatenation function.
 * <p>
 * These tests verify that the runtime-generated variadic evaluator works correctly
 * for functions that take a variable number of BytesRef (string) arguments.
 */
public class Concat2RestIT extends ESRestTestCase {

    private static final String TEST_INDEX = "test_concat2";

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
        Request createIndex = new Request("PUT", "/" + TEST_INDEX);
        createIndex.setJsonEntity("""
            {
                "mappings": {
                    "properties": {
                        "a": { "type": "keyword" },
                        "b": { "type": "keyword" },
                        "c": { "type": "keyword" }
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
        request.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE).build());
        Response response = client().performRequest(request);
        String responseBody = EntityUtils.toString(response.getEntity());
        return org.elasticsearch.xcontent.json.JsonXContent.jsonXContent.createParser(
            org.elasticsearch.xcontent.XContentParserConfiguration.EMPTY,
            responseBody
        ).map();
    }

    // ==================== Tests ====================

    public void testBasicConcatenation() throws IOException {
        Map<String, Object> result = runQuery(
            "ROW a = \\\"Hello\\\", b = \\\" \\\", c = \\\"World\\\" | EVAL result = concat2(a, b, c) | KEEP result"
        );

        @SuppressWarnings("unchecked")
        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));
        assertThat(values.get(0).get(0), equalTo("Hello World"));
    }

    public void testTwoStrings() throws IOException {
        Map<String, Object> result = runQuery("ROW x = \\\"foo\\\", y = \\\"bar\\\" | EVAL z = concat2(x, y) | KEEP z");

        @SuppressWarnings("unchecked")
        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));
        assertThat(values.get(0).get(0), equalTo("foobar"));
    }

    public void testEmptyStrings() throws IOException {
        Map<String, Object> result = runQuery("ROW a = \\\"\\\", b = \\\"test\\\" | EVAL c = concat2(a, b) | KEEP c");

        @SuppressWarnings("unchecked")
        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));
        assertThat(values.get(0).get(0), equalTo("test"));
    }

    public void testSingleArgument() throws IOException {
        Map<String, Object> result = runQuery("ROW a = \\\"solo\\\" | EVAL b = concat2(a) | KEEP b");

        @SuppressWarnings("unchecked")
        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));
        assertThat(values.get(0).get(0), equalTo("solo"));
    }

    public void testManyArguments() throws IOException {
        Map<String, Object> result = runQuery(
            "ROW a = \\\"a\\\", b = \\\"b\\\", c = \\\"c\\\", d = \\\"d\\\", e = \\\"e\\\" "
                + "| EVAL result = concat2(a, b, c, d, e) | KEEP result"
        );

        @SuppressWarnings("unchecked")
        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));
        assertThat(values.get(0).get(0), equalTo("abcde"));
    }

    public void testWithIndexedData() throws IOException {
        StringBuilder bulk = new StringBuilder();
        bulk.append("{\"index\": {\"_id\": \"1\"}}\n");
        bulk.append("{\"a\": \"Hello\", \"b\": \" \", \"c\": \"World\"}\n");
        bulk.append("{\"index\": {\"_id\": \"2\"}}\n");
        bulk.append("{\"a\": \"foo\", \"b\": \"bar\", \"c\": \"baz\"}\n");
        bulk.append("{\"index\": {\"_id\": \"3\"}}\n");
        bulk.append("{\"a\": \"ES\", \"b\": \"|\", \"c\": \"QL\"}\n");

        Request bulkRequest = new Request("POST", "/" + TEST_INDEX + "/_bulk");
        bulkRequest.setJsonEntity(bulk.toString());
        bulkRequest.addParameter("refresh", "true");
        client().performRequest(bulkRequest);

        Map<String, Object> result = runQuery(
            "FROM " + TEST_INDEX + " | EVAL result = concat2(a, b, c) | SORT result | KEEP a, b, c, result"
        );

        @SuppressWarnings("unchecked")
        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(3));

        assertThat(values.get(0).get(3), equalTo("ES|QL"));
        assertThat(values.get(1).get(3), equalTo("Hello World"));
        assertThat(values.get(2).get(3), equalTo("foobarbaz"));
    }

    public void testWithNullValues() throws IOException {
        StringBuilder bulk = new StringBuilder();
        bulk.append("{\"index\": {\"_id\": \"1\"}}\n");
        bulk.append("{\"a\": \"Hello\", \"b\": \" \", \"c\": \"World\"}\n");
        bulk.append("{\"index\": {\"_id\": \"2\"}}\n");
        bulk.append("{\"a\": \"foo\", \"b\": null, \"c\": \"baz\"}\n");
        bulk.append("{\"index\": {\"_id\": \"3\"}}\n");
        bulk.append("{\"a\": \"test\", \"b\": \"value\"}\n");

        Request bulkRequest = new Request("POST", "/" + TEST_INDEX + "/_bulk");
        bulkRequest.setJsonEntity(bulk.toString());
        bulkRequest.addParameter("refresh", "true");
        client().performRequest(bulkRequest);

        Map<String, Object> result = runQuery("FROM " + TEST_INDEX + " | EVAL result = concat2(a, b, c) | SORT a | KEEP a, b, c, result");

        @SuppressWarnings("unchecked")
        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(3));

        assertThat(values.get(0).get(3), equalTo("Hello World"));
        assertThat(values.get(1).get(3), equalTo(null));
        assertThat(values.get(2).get(3), equalTo(null));
    }

    public void testMatchesBuiltInConcat() throws IOException {
        Map<String, Object> result = runQuery(
            "ROW a = \\\"Hello\\\", b = \\\" \\\", c = \\\"World\\\" "
                + "| EVAL c2 = concat2(a, b, c), builtin = CONCAT(a, b, c) "
                + "| KEEP c2, builtin"
        );

        @SuppressWarnings("unchecked")
        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));
        String c2 = (String) values.get(0).get(0);
        String builtin = (String) values.get(0).get(1);
        assertThat(c2, equalTo(builtin));
        assertThat(c2, equalTo("Hello World"));
    }

    public void testAllEmptyStrings() throws IOException {
        Map<String, Object> result = runQuery(
            "ROW a = \\\"\\\", b = \\\"\\\", c = \\\"\\\" | EVAL result = concat2(a, b, c) | KEEP result"
        );

        @SuppressWarnings("unchecked")
        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));
        assertThat(values.get(0).get(0), equalTo(""));
    }
}
