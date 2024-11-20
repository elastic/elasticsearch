/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.qa;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class AsyncSearchHeadersIT extends ESRestTestCase {
    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local().plugin("x-pack-async-search").build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Before
    public void createIndex() throws IOException {
        client().performRequest(new Request("PUT", "/test_index"));
    }

    public void testAsyncHeaders() throws IOException {
        Response submitResponse = client().performRequest(new Request("POST", "/test_index/_async_search?keep_on_completion=true"));
        var asyncExecutionId = assertAsyncHeaders(submitResponse);

        Response statusResponse = client().performRequest(new Request("GET", "/_async_search/status/" + asyncExecutionId));
        assertAsyncHeaders(statusResponse);

        Response resultResponse = client().performRequest(new Request("GET", "/_async_search/" + asyncExecutionId));
        assertAsyncHeaders(resultResponse);
    }

    private String assertAsyncHeaders(Response response) throws IOException {
        var json = entityAsMap(response);

        var asyncExecutionId = (String) json.get("id");
        var isRunning = (boolean) json.get("is_running");

        if (asyncExecutionId != null) {
            assertThat(response.getHeader("X-ElasticSearch-Async-Id"), equalTo(asyncExecutionId));
        }
        assertThat(response.getHeader("X-ElasticSearch-Async-Is-Running"), equalTo(isRunning ? "?1" : "?0"));

        return asyncExecutionId;
    }
}
