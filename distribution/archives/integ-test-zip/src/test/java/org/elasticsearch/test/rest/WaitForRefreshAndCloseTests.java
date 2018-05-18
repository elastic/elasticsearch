/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.test.rest;

import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.ResponseListener;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static java.util.Collections.emptyMap;

/**
 * Tests that wait for refresh is fired if the index is closed.
 */
public class WaitForRefreshAndCloseTests extends ESRestTestCase {
    @Before
    public void setupIndex() throws IOException {
        try {
            client().performRequest("DELETE", indexName());
        } catch (ResponseException e) {
            // If we get an error, it should be because the index doesn't exist
            assertEquals(404, e.getResponse().getStatusLine().getStatusCode());
        }
        client().performRequest("PUT", indexName(), emptyMap(),
            new StringEntity("{\"settings\":{\"refresh_interval\":-1}}", ContentType.APPLICATION_JSON));
    }

    @After
    public void cleanupIndex() throws IOException {
        client().performRequest("DELETE", indexName());
    }

    private String indexName() {
        return getTestName().toLowerCase(Locale.ROOT);
    }

    private String docPath() {
        return indexName() + "/test/1";
    }

    public void testIndexAndThenClose() throws Exception {
        closeWhileListenerEngaged(start("PUT", "", new StringEntity("{\"test\":\"test\"}", ContentType.APPLICATION_JSON)));
    }

    public void testUpdateAndThenClose() throws Exception {
        client().performRequest("PUT", docPath(), emptyMap(), new StringEntity("{\"test\":\"test\"}", ContentType.APPLICATION_JSON));
        closeWhileListenerEngaged(start("POST", "/_update",
            new StringEntity("{\"doc\":{\"name\":\"test\"}}", ContentType.APPLICATION_JSON)));
    }

    public void testDeleteAndThenClose() throws Exception {
        client().performRequest("PUT", docPath(), emptyMap(), new StringEntity("{\"test\":\"test\"}", ContentType.APPLICATION_JSON));
        closeWhileListenerEngaged(start("DELETE", "", null));
    }

    private void closeWhileListenerEngaged(ActionFuture<String> future) throws Exception {
        // Wait for the refresh listener to start waiting
        assertBusy(() -> {
            Map<String, Object> stats;
            try {
                stats = entityAsMap(client().performRequest("GET", indexName() + "/_stats/refresh"));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> indices = (Map<String, Object>) stats.get("indices");
            @SuppressWarnings("unchecked")
            Map<String, Object> theIndex = (Map<String, Object>) indices.get(indexName());
            @SuppressWarnings("unchecked")
            Map<String, Object> total = (Map<String, Object>) theIndex.get("total");
            @SuppressWarnings("unchecked")
            Map<String, Object> refresh = (Map<String, Object>) total.get("refresh");
            int listeners = (int) refresh.get("listeners");
            assertEquals(1, listeners);
        });

        // Close the index. That should flush the listener.
        client().performRequest("POST", indexName() + "/_close");

        // The request shouldn't fail. It certainly shouldn't hang.
        future.get();
    }

    private ActionFuture<String> start(String method, String path, HttpEntity body) {
        PlainActionFuture<String> future = new PlainActionFuture<>();
        Map<String, String> params = new HashMap<>();
        params.put("refresh", "wait_for");
        params.put("error_trace", "");
        client().performRequestAsync(method, docPath() + path, params, body, new ResponseListener() {
            @Override
            public void onSuccess(Response response) {
                try {
                    future.onResponse(EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8));
                } catch (IOException e) {
                    future.onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception exception) {
                future.onFailure(exception);
            }
        });
        return future;
    }
}
