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
 * REST integration tests for the Sum2 aggregate function.
 * <p>
 * Tests that Sum2 works correctly with runtime-generated aggregator bytecode.
 * </p>
 */
public class Sum2RestIT extends ESRestTestCase {

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
    private Map<String, Object> runQuery(String query) throws IOException {
        Request request = new Request("POST", "/_query");
        request.setJsonEntity("{\"query\": \"" + query + "\"}");
        RequestOptions options = RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE).build();
        request.setOptions(options);
        Response response = client().performRequest(request);
        return entityAsMap(response);
    }

    public void testSum2Basic() throws IOException {
        createTestIndex();
        indexTestData();

        Map<String, Object> responseMap = runQuery("FROM test_sum2 | STATS total = sum2(value)");

        @SuppressWarnings("unchecked")
        List<List<Object>> values = (List<List<Object>>) responseMap.get("values");
        assertThat(values, hasSize(1));
        assertThat(values.get(0), hasSize(1));
        assertThat(((Number) values.get(0).get(0)).longValue(), equalTo(15L));
    }

    public void testSum2WithGroupBy() throws IOException {
        createTestIndex();
        indexTestDataWithCategories();

        Map<String, Object> responseMap = runQuery("FROM test_sum2 | STATS total = sum2(value) BY category | SORT category");

        @SuppressWarnings("unchecked")
        List<List<Object>> values = (List<List<Object>>) responseMap.get("values");
        assertThat(values, hasSize(2));
        assertThat(((Number) values.get(0).get(0)).longValue(), equalTo(6L));
        assertThat(values.get(0).get(1), equalTo("A"));
        assertThat(((Number) values.get(1).get(0)).longValue(), equalTo(9L));
        assertThat(values.get(1).get(1), equalTo("B"));
    }

    public void testSum2EmptyResult() throws IOException {
        createTestIndex();

        Map<String, Object> responseMap = runQuery("FROM test_sum2 | WHERE value > 1000 | STATS total = sum2(value)");

        @SuppressWarnings("unchecked")
        List<List<Object>> values = (List<List<Object>>) responseMap.get("values");
        assertThat(values, hasSize(1));
        assertThat(values.get(0), hasSize(1));
        assertNull(values.get(0).get(0));
    }

    private void createTestIndex() throws IOException {
        Request deleteRequest = new Request("DELETE", "/test_sum2");
        deleteRequest.addParameter("ignore_unavailable", "true");
        client().performRequest(deleteRequest);

        Request createRequest = new Request("PUT", "/test_sum2");
        createRequest.setJsonEntity("""
            {
              "mappings": {
                "properties": {
                  "value": { "type": "integer" },
                  "category": { "type": "keyword" }
                }
              }
            }
            """);
        client().performRequest(createRequest);
    }

    private void indexTestData() throws IOException {
        Request bulkRequest = new Request("POST", "/test_sum2/_bulk?refresh=true");
        bulkRequest.setJsonEntity("""
            {"index":{}}
            {"value": 1}
            {"index":{}}
            {"value": 2}
            {"index":{}}
            {"value": 3}
            {"index":{}}
            {"value": 4}
            {"index":{}}
            {"value": 5}
            """);
        client().performRequest(bulkRequest);
    }

    private void indexTestDataWithCategories() throws IOException {
        Request bulkRequest = new Request("POST", "/test_sum2/_bulk?refresh=true");
        bulkRequest.setJsonEntity("""
            {"index":{}}
            {"value": 1, "category": "A"}
            {"index":{}}
            {"value": 2, "category": "A"}
            {"index":{}}
            {"value": 3, "category": "A"}
            {"index":{}}
            {"value": 4, "category": "B"}
            {"index":{}}
            {"value": 5, "category": "B"}
            """);
        client().performRequest(bulkRequest);
    }
}
