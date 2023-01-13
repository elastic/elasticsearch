/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.remotecluster;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * This test suite will be run twice: Once against the fulfilling cluster, then again against the querying cluster.
 */
public class CcsViaHttpIT extends ESRestTestCase {

    private static final String USER = "test_user";
    private static final SecureString PASS = new SecureString("x-pack-test-password".toCharArray());

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveDataStreamsUponCompletion() {
        return true;
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(USER, PASS);
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    private boolean isFulfillingCluster() {
        return "fulfilling_cluster".equals(System.getProperty("tests.rest.suite"));
    }

    public void testRemoteAccessTransportSearchActionApproach() throws Exception {
        if (isFulfillingCluster()) {
            // Index some documents, so we can search them from the querying cluster
            Request indexDocRequest = new Request("POST", "/test_idx/_doc");
            indexDocRequest.setJsonEntity("{\"foo\": \"bar\"}");
            Response response = client().performRequest(indexDocRequest);
            assertOK(response);

        } else {
            // Check that we can search the fulfilling cluster from the querying cluster
            // CCS via HTTP - directly send HTTP requests in TransportSearchAction

            Request searchRequest = new Request("GET", "/my_remote_cluster:test_idx/_search");
            Response response = client().performRequest(searchRequest);
            assertSearchResponse(response, 1);
        }
    }

    public void testRemote2ShardsWithSecurityServerTransportInterceptorApproach() throws IOException {
        if (isFulfillingCluster()) {
            final Request createIndexRequest = new Request("PUT", "/index_with_2_shards");
            createIndexRequest.setJsonEntity("""
                {
                  "settings": {
                     "number_of_replicas": 0,
                     "number_of_shards": 2
                   }
                }""");
            assertOK(client().performRequest(createIndexRequest));

            // Index multiple documents so they are likely spread onto different shard/node
            final Request bulkRequest = new Request("POST", "/_bulk");
            bulkRequest.setJsonEntity("""
                {"index": {"_index": "index_with_2_shards"}}
                {"foo": "bar1"}
                {"index": {"_index": "index_with_2_shards"}}
                {"foo": "bar2"}
                {"index": {"_index": "index_with_2_shards"}}
                {"foo": "bar3"}
                {"index": {"_index": "index_with_2_shards"}}
                {"foo": "bar4"}
                """);
            assertOK(client().performRequest(bulkRequest));
        } else {
            // Check that we can search the fulfilling cluster from the querying cluster
            // CCS via HTTP - intercept and send HTTP requests in SecurityServerTransportInterceptor

            // Search with or without minimizing roundtrips
            final Request searchRequest = new Request("GET", "/my_other_remote*:index_with_2*/_search");
            searchRequest.addParameter("ccs_minimize_roundtrips", String.valueOf(randomBoolean()));
            assertSearchResponse(client().performRequest(searchRequest), 4);

            // Search with PIT
            final Request openPitRequest = new Request("POST", "/my_other_remote*:index_with_2*/_pit");
            openPitRequest.addParameter("keep_alive", "1h");
            final Response openPitResponse = client().performRequest(openPitRequest);
            assertOK(openPitResponse);
            final String pitId = ObjectPath.createFromResponse(openPitResponse).evaluate("id");
            assertThat(pitId, notNullValue());
            final Request pitSearchRequest = new Request("GET", "/_search");
            pitSearchRequest.setJsonEntity("""
                {
                  "pit": {
                    "id": "%s",
                    "keep_alive": "1h"
                  }
                }""".formatted(pitId));
            assertSearchResponse(client().performRequest(pitSearchRequest), 4);

            // Search with scroll
            final Request openScrollRequest = new Request("POST", "/my_other_remote*:index_with_2*/_search");
            openScrollRequest.addParameters(Map.of("scroll", "1h", "size", "1"));
            final String scrollId = ObjectPath.createFromResponse(client().performRequest(openScrollRequest)).evaluate("_scroll_id");
            assertThat(scrollId, notNullValue());
            final Request scrollSearchRequest = new Request("GET", "/_search/scroll");
            scrollSearchRequest.setJsonEntity("""
                {
                  "scroll_id": "%s"
                }""".formatted(scrollId));
            assertSearchResponse(client().performRequest(scrollSearchRequest), 4);

            // Async Search
            final Request startAsyncSearchRequest = new Request("POST", "/my_other_remote*:index_with_2*/_async_search");
            startAsyncSearchRequest.addParameters(Map.of("keep_on_completion", "true", "keep_alive", "1h"));
            final Object asyncSearchId = ObjectPath.createFromResponse(client().performRequest(startAsyncSearchRequest)).evaluate("id");
            assertThat(asyncSearchId, notNullValue());
            final Request getAsyncSearchRequest = new Request("GET", "/_async_search/" + asyncSearchId);
            final Response getAsyncSearchResponse = client().performRequest(getAsyncSearchRequest);
            assertOK(getAsyncSearchResponse);
            ObjectPath responseObj = ObjectPath.createFromResponse(getAsyncSearchResponse);
            assertThat(responseObj.evaluate("response.hits.total.value"), equalTo(4));
        }
    }

    private static void assertSearchResponse(Response searchResponse, int nhits) throws IOException {
        assertOK(searchResponse);
        ObjectPath responseObj = ObjectPath.createFromResponse(searchResponse);
        int totalHits = responseObj.evaluate("hits.total.value");
        assertThat(totalHits, equalTo(nhits));
    }
}
