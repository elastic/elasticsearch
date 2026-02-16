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
 * REST integration tests for the MaxBytes2 aggregate function.
 * Tests BytesRef state with runtime-generated aggregator bytecode using custom AggregatorState.
 */
public class MaxBytes2RestIT extends ESRestTestCase {

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

    public void testMaxBytes2Basic() throws IOException {
        createTestIndex();
        indexTestData();

        // Max of ["apple", "zebra", "banana", "cherry", "mango"] = "zebra"
        Map<String, Object> responseMap = runQuery("FROM test_max_bytes2 | STATS max_name = max_bytes2(name)");

        @SuppressWarnings("unchecked")
        List<List<Object>> values = (List<List<Object>>) responseMap.get("values");
        assertThat(values, hasSize(1));
        assertThat(values.get(0), hasSize(1));
        assertThat(values.get(0).get(0), equalTo("zebra"));
    }

    public void testMaxBytes2WithGroupBy() throws IOException {
        createTestIndex();
        indexTestDataWithCategories();

        Map<String, Object> responseMap = runQuery(
            "FROM test_max_bytes2 | STATS max_name = max_bytes2(name) BY category | SORT category"
        );

        @SuppressWarnings("unchecked")
        List<List<Object>> values = (List<List<Object>>) responseMap.get("values");
        assertThat(values, hasSize(2));
        // Category A: max of ["apple", "banana"] = "banana"
        assertThat(values.get(0).get(0), equalTo("banana"));
        assertThat(values.get(0).get(1), equalTo("A"));
        // Category B: max of ["zebra", "cherry", "mango"] = "zebra"
        assertThat(values.get(1).get(0), equalTo("zebra"));
        assertThat(values.get(1).get(1), equalTo("B"));
    }

    public void testMaxBytes2EmptyResult() throws IOException {
        createTestIndex();

        Map<String, Object> responseMap = runQuery(
            "FROM test_max_bytes2 | WHERE name == \\\"nonexistent\\\" | STATS max_name = max_bytes2(name)"
        );

        @SuppressWarnings("unchecked")
        List<List<Object>> values = (List<List<Object>>) responseMap.get("values");
        assertThat(values, hasSize(1));
        assertThat(values.get(0), hasSize(1));
        assertNull(values.get(0).get(0));
    }

    public void testMaxBytes2SingleValue() throws IOException {
        createTestIndex();
        indexSingleValue();

        Map<String, Object> responseMap = runQuery("FROM test_max_bytes2 | STATS max_name = max_bytes2(name)");

        @SuppressWarnings("unchecked")
        List<List<Object>> values = (List<List<Object>>) responseMap.get("values");
        assertThat(values, hasSize(1));
        assertThat(values.get(0), hasSize(1));
        assertThat(values.get(0).get(0), equalTo("only_value"));
    }

    private void createTestIndex() throws IOException {
        Request deleteRequest = new Request("DELETE", "/test_max_bytes2");
        deleteRequest.addParameter("ignore_unavailable", "true");
        client().performRequest(deleteRequest);

        Request createRequest = new Request("PUT", "/test_max_bytes2");
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
        Request bulkRequest = new Request("POST", "/test_max_bytes2/_bulk?refresh=true");
        bulkRequest.setJsonEntity("""
            {"index":{}}
            {"name": "apple"}
            {"index":{}}
            {"name": "zebra"}
            {"index":{}}
            {"name": "banana"}
            {"index":{}}
            {"name": "cherry"}
            {"index":{}}
            {"name": "mango"}
            """);
        client().performRequest(bulkRequest);
    }

    private void indexTestDataWithCategories() throws IOException {
        Request bulkRequest = new Request("POST", "/test_max_bytes2/_bulk?refresh=true");
        bulkRequest.setJsonEntity("""
            {"index":{}}
            {"name": "apple", "category": "A"}
            {"index":{}}
            {"name": "banana", "category": "A"}
            {"index":{}}
            {"name": "zebra", "category": "B"}
            {"index":{}}
            {"name": "cherry", "category": "B"}
            {"index":{}}
            {"name": "mango", "category": "B"}
            """);
        client().performRequest(bulkRequest);
    }

    private void indexSingleValue() throws IOException {
        Request bulkRequest = new Request("POST", "/test_max_bytes2/_bulk?refresh=true");
        bulkRequest.setJsonEntity("""
            {"index":{}}
            {"name": "only_value"}
            """);
        client().performRequest(bulkRequest);
    }
}
