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
 * REST integration tests for the MvAvg2 function.
 * Tests runtime MvEvaluator generation with single value optimization.
 */
public class MvAvg2RestIT extends ESRestTestCase {

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
        Request createIndex = new Request("PUT", "/test_mv_avg2");
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

        Request bulkRequest = new Request("POST", "/test_mv_avg2/_bulk?refresh=true");
        bulkRequest.setJsonEntity("""
            {"index":{}}
            {"values": [1, 2, 3], "doubles": [1.0, 2.0, 3.0], "longs": [10, 20, 30]}
            {"index":{}}
            {"values": [10], "doubles": [10.0], "longs": [100]}
            {"index":{}}
            {"values": [5, 5], "doubles": [5.0, 5.0], "longs": [50, 50]}
            """);
        client().performRequest(bulkRequest);
    }

    @After
    public void cleanupIndex() throws IOException {
        Request deleteIndex = new Request("DELETE", "/test_mv_avg2");
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
    public void testMvAvg2IntBasic() throws IOException {
        Map<String, Object> result = runQuery("ROW x = [1, 2, 3] | EVAL sum = mv_avg2(x)");
        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));
        assertThat(values.get(0).get(1), equalTo(6));
    }

    @SuppressWarnings("unchecked")
    public void testMvAvg2IntSingleValue() throws IOException {
        Map<String, Object> result = runQuery("ROW x = [42] | EVAL sum = mv_avg2(x)");
        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));
        assertThat(values.get(0).get(1), equalTo(42));
    }

    @SuppressWarnings("unchecked")
    public void testMvAvg2Double() throws IOException {
        Map<String, Object> result = runQuery("ROW x = [1.0, 2.0, 3.0] | EVAL sum = mv_avg2(x)");
        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));
        assertThat(values.get(0).get(1), equalTo(6.0));
    }

    @SuppressWarnings("unchecked")
    public void testMvAvg2Long() throws IOException {
        Map<String, Object> result = runQuery("ROW x = [10, 20, 30]::long | EVAL sum = mv_avg2(x)");
        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));
        assertThat(values.get(0).get(1), equalTo(60));
    }

    @SuppressWarnings("unchecked")
    public void testMvAvg2FromIndex() throws IOException {
        Map<String, Object> result = runQuery("FROM test_mv_avg2 | EVAL sum = mv_avg2(values) | KEEP sum | SORT sum");
        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(3));
        assertThat(values.get(0).get(0), equalTo(6));
        assertThat(values.get(1).get(0), equalTo(10));
    }

    @SuppressWarnings("unchecked")
    public void testMvAvg2SingleValueFromIndex() throws IOException {
        Map<String, Object> result = runQuery("FROM test_mv_avg2 | WHERE values == 10 | EVAL sum = mv_avg2(values) | KEEP sum");
        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));
        assertThat(values.get(0).get(0), equalTo(10));
    }
}
