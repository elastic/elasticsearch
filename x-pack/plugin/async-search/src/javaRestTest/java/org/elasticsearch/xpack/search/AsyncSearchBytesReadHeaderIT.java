/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.search;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class AsyncSearchBytesReadHeaderIT extends ESRestTestCase {

    private static final String BYTES_READ_HEADER = "X-Elasticsearch-Bytes-Read";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local().module("x-pack-async-search").build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Before
    public void setup() throws IOException {
        assumeTrue("directory metrics must be enabled", Store.DIRECTORY_METRICS_FEATURE_FLAG.isEnabled());

        Request createIndex = new Request("PUT", "/test-bytes-read");
        createIndex.setJsonEntity("""
            {
                "settings": { "number_of_shards": 1, "number_of_replicas": 0 }
            }
            """);
        client().performRequest(createIndex);

        Request bulk = new Request("POST", "/_bulk?refresh=true");
        StringBuilder body = new StringBuilder();
        for (int i = 0; i < 50; i++) {
            body.append("{ \"index\": { \"_index\": \"test-bytes-read\" } }\n");
            body.append("{ \"field\": \"value").append(i).append("\" }\n");
        }
        bulk.setJsonEntity(body.toString());
        client().performRequest(bulk);
    }

    public void testBytesReadHeaderOnSubmitWithSyncCompletion() throws IOException {
        Request submit = new Request("POST", "/test-bytes-read/_async_search");
        submit.addParameter("wait_for_completion_timeout", "10s");
        submit.addParameter("keep_on_completion", "false");
        submit.setJsonEntity("""
            { "query": { "match_all": {} }, "size": 1, "sort": [ { "field.keyword": "desc" } ] }
            """);

        Response response = client().performRequest(submit);
        assertOK(response);

        String bytesRead = response.getHeader(BYTES_READ_HEADER);
        assertThat(bytesRead, notNullValue());
        assertThat(Long.parseLong(bytesRead), greaterThan(0L));
    }

    public void testBytesReadHeaderOnSubmitAndGet() throws IOException {
        Request submit = new Request("POST", "/test-bytes-read/_async_search");
        submit.addParameter("wait_for_completion_timeout", "10s");
        submit.addParameter("keep_on_completion", "true");
        submit.setJsonEntity("""
            { "query": { "match_all": {} }, "size": 1, "sort": [ { "field.keyword": "desc" } ] }
            """);

        Response submitResponse = client().performRequest(submit);
        assertOK(submitResponse);

        String bytesReadOnSubmit = submitResponse.getHeader(BYTES_READ_HEADER);
        assertThat(bytesReadOnSubmit, notNullValue());
        long submitBytesRead = Long.parseLong(bytesReadOnSubmit);
        assertThat(submitBytesRead, greaterThan(0L));

        var submitBody = entityAsMap(submitResponse);
        assertThat(submitBody.get("is_running"), equalTo(false));
        String asyncId = (String) submitBody.get("id");
        assertThat(asyncId, notNullValue());

        try {
            Response getResponse = client().performRequest(new Request("GET", "/_async_search/" + asyncId));
            assertOK(getResponse);

            String bytesReadOnGet = getResponse.getHeader(BYTES_READ_HEADER);
            assertThat(bytesReadOnGet, notNullValue());
            long getBytesRead = Long.parseLong(bytesReadOnGet);
            assertThat(getBytesRead, greaterThan(0L));
            assertThat(getBytesRead, equalTo(submitBytesRead));
        } finally {
            client().performRequest(new Request("DELETE", "/_async_search/" + asyncId));
        }
    }

    public void testBytesReadHeaderAbsentOnStatus() throws IOException {
        Request submit = new Request("POST", "/test-bytes-read/_async_search");
        submit.addParameter("wait_for_completion_timeout", "10s");
        submit.addParameter("keep_on_completion", "true");
        submit.setJsonEntity("""
            { "query": { "match_all": {} } }
            """);

        Response submitResponse = client().performRequest(submit);
        assertOK(submitResponse);

        var submitBody = entityAsMap(submitResponse);
        String asyncId = (String) submitBody.get("id");
        assertThat(asyncId, notNullValue());

        try {
            Response statusResponse = client().performRequest(new Request("GET", "/_async_search/status/" + asyncId));
            assertOK(statusResponse);

            assertThat(statusResponse.getHeader(BYTES_READ_HEADER), nullValue());
        } finally {
            client().performRequest(new Request("DELETE", "/_async_search/" + asyncId));
        }
    }
}
