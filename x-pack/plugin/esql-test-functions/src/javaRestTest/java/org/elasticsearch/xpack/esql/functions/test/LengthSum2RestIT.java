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

/**
 * REST integration tests for the LengthSum2 aggregate function.
 * Tests BytesRef input with runtime-generated aggregator bytecode.
 */
public class LengthSum2RestIT extends ESRestTestCase {

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

    private Map<String, Object> runQuery(String query) throws IOException {
        Request request = new Request("POST", "/_query");
        request.setJsonEntity("{\"query\": \"" + query + "\"}");
        RequestOptions options = RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE).build();
        request.setOptions(options);
        Response response = client().performRequest(request);
        return entityAsMap(response);
    }

    public void testLengthSum2Basic() throws IOException {
        createTestIndex();
        indexTestData();

        // "hello" (5) + "world" (5) + "foo" (3) + "bar" (3) + "test" (4) = 20
        Map<String, Object> responseMap = runQuery("FROM test_length_sum2 | STATS total = length_sum2(name)");

        @SuppressWarnings("unchecked")
        List<List<Object>> values = (List<List<Object>>) responseMap.get("values");
        assertThat(values, hasSize(1));
        assertThat(values.get(0), hasSize(1));
        assertThat(((Number) values.get(0).get(0)).longValue(), equalTo(20L));
    }

    public void testLengthSum2WithGroupBy() throws IOException {
        createTestIndex();
        indexTestDataWithCategories();

        Map<String, Object> responseMap = runQuery("FROM test_length_sum2 | STATS total = length_sum2(name) BY category | SORT category");

        @SuppressWarnings("unchecked")
        List<List<Object>> values = (List<List<Object>>) responseMap.get("values");
        assertThat(values, hasSize(2));
        // Category A: "hello" (5) + "world" (5) = 10
        assertThat(((Number) values.get(0).get(0)).longValue(), equalTo(10L));
        assertThat(values.get(0).get(1), equalTo("A"));
        // Category B: "foo" (3) + "bar" (3) + "test" (4) = 10
        assertThat(((Number) values.get(1).get(0)).longValue(), equalTo(10L));
        assertThat(values.get(1).get(1), equalTo("B"));
    }

    public void testLengthSum2EmptyResult() throws IOException {
        createTestIndex();

        Map<String, Object> responseMap = runQuery(
            "FROM test_length_sum2 | WHERE name == \\\"nonexistent\\\" | STATS total = length_sum2(name)"
        );

        @SuppressWarnings("unchecked")
        List<List<Object>> values = (List<List<Object>>) responseMap.get("values");
        assertThat(values, hasSize(1));
        assertThat(values.get(0), hasSize(1));
        assertNull(values.get(0).get(0));
    }

    private void createTestIndex() throws IOException {
        Request deleteRequest = new Request("DELETE", "/test_length_sum2");
        deleteRequest.addParameter("ignore_unavailable", "true");
        client().performRequest(deleteRequest);

        Request createRequest = new Request("PUT", "/test_length_sum2");
        createRequest.setJsonEntity("""
            {
              "mappings": {
                "properties": {
                  "name": { "type": "keyword" },
                  "category": { "type": "keyword" }
                }
              }
            }
            """);
        client().performRequest(createRequest);
    }

    private void indexTestData() throws IOException {
        Request bulkRequest = new Request("POST", "/test_length_sum2/_bulk?refresh=true");
        bulkRequest.setJsonEntity("""
            {"index":{}}
            {"name": "hello"}
            {"index":{}}
            {"name": "world"}
            {"index":{}}
            {"name": "foo"}
            {"index":{}}
            {"name": "bar"}
            {"index":{}}
            {"name": "test"}
            """);
        client().performRequest(bulkRequest);
    }

    private void indexTestDataWithCategories() throws IOException {
        Request bulkRequest = new Request("POST", "/test_length_sum2/_bulk?refresh=true");
        bulkRequest.setJsonEntity("""
            {"index":{}}
            {"name": "hello", "category": "A"}
            {"index":{}}
            {"name": "world", "category": "A"}
            {"index":{}}
            {"name": "foo", "category": "B"}
            {"index":{}}
            {"name": "bar", "category": "B"}
            {"index":{}}
            {"name": "test", "category": "B"}
            """);
        client().performRequest(bulkRequest);
    }
}
