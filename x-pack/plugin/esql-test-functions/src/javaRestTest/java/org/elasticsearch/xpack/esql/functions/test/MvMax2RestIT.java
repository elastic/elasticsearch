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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * REST integration tests for the MvMax2 function.
 * Tests runtime MvEvaluator generation with ascending optimization.
 */
public class MvMax2RestIT extends ESRestTestCase {

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
        Request createIndex = new Request("PUT", "/test_mv_max2");
        createIndex.setJsonEntity("""
            {
                "mappings": {
                    "properties": {
                        "values": { "type": "integer" },
                        "doubles": { "type": "double" },
                        "longs": { "type": "long" }
                    }
                }
            }
            """);
        client().performRequest(createIndex);

        Request bulkRequest = new Request("POST", "/test_mv_max2/_bulk?refresh=true");
        bulkRequest.setJsonEntity("""
            {"index":{}}
            {"values": [1, 5, 3], "doubles": [1.1, 5.5, 3.3], "longs": [10, 50, 30]}
            {"index":{}}
            {"values": [10, 20, 30], "doubles": [10.0, 20.0, 30.0], "longs": [100, 200, 300]}
            {"index":{}}
            {"values": [7], "doubles": [7.7], "longs": [77]}
            """);
        client().performRequest(bulkRequest);
    }

    @After
    public void cleanupIndex() throws IOException {
        Request deleteIndex = new Request("DELETE", "/test_mv_max2");
        deleteIndex.addParameter("ignore_unavailable", "true");
        client().performRequest(deleteIndex);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> runQuery(String query) throws IOException {
        Request request = new Request("POST", "/_query");
        request.setJsonEntity("{\"query\": \"" + query + "\"}");
        request.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE).build());
        Response response = client().performRequest(request);
        return entityAsMap(response);
    }

    @SuppressWarnings("unchecked")
    public void testMvMax2IntBasic() throws IOException {
        Map<String, Object> result = runQuery("ROW x = [1, 5, 3] | EVAL max = mv_max2(x)");
        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));
        assertThat(values.get(0).get(1), equalTo(5));
    }

    @SuppressWarnings("unchecked")
    public void testMvMax2IntFromIndex() throws IOException {
        Map<String, Object> result = runQuery("FROM test_mv_max2 | EVAL max = mv_max2(values) | KEEP max | SORT max");
        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(3));
        assertThat(values.get(0).get(0), equalTo(5));
        assertThat(values.get(1).get(0), equalTo(7));
        assertThat(values.get(2).get(0), equalTo(30));
    }

    @SuppressWarnings("unchecked")
    public void testMvMax2Double() throws IOException {
        Map<String, Object> result = runQuery("ROW x = [1.1, 5.5, 3.3] | EVAL max = mv_max2(x)");
        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));
        assertThat(values.get(0).get(1), equalTo(5.5));
    }

    @SuppressWarnings("unchecked")
    public void testMvMax2Long() throws IOException {
        Map<String, Object> result = runQuery("ROW x = [10, 50, 30]::long | EVAL max = mv_max2(x)");
        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));
        assertThat(values.get(0).get(1), equalTo(50));
    }

    @SuppressWarnings("unchecked")
    public void testMvMax2SingleValue() throws IOException {
        Map<String, Object> result = runQuery("ROW x = [42] | EVAL max = mv_max2(x)");
        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));
        assertThat(values.get(0).get(1), equalTo(42));
    }

    @SuppressWarnings("unchecked")
    public void testMvMax2CompareWithBuiltin() throws IOException {
        Map<String, Object> result = runQuery(
            "FROM test_mv_max2 | EVAL max1 = mv_max(values), max2 = mv_max2(values) | KEEP max1, max2 | SORT max1"
        );
        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(3));
        assertThat(values.get(0).get(0), equalTo(values.get(0).get(1)));
        assertThat(values.get(1).get(0), equalTo(values.get(1).get(1)));
        assertThat(values.get(2).get(0), equalTo(values.get(2).get(1)));
    }
}
