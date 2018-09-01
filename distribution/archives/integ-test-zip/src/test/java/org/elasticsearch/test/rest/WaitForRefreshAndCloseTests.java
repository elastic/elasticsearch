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

import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.client.Request;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map;

/**
 * Tests that wait for refresh is fired if the index is closed.
 */
public class WaitForRefreshAndCloseTests extends ESRestTestCase {
    @Before
    public void setupIndex() throws IOException {
        try {
            client().performRequest(new Request("DELETE", indexName()));
        } catch (ResponseException e) {
            // If we get an error, it should be because the index doesn't exist
            assertEquals(404, e.getResponse().getStatusLine().getStatusCode());
        }
        Request request = new Request("PUT", indexName());
        request.setJsonEntity("{\"settings\":{\"refresh_interval\":-1}}");
        client().performRequest(request);
    }

    @After
    public void cleanupIndex() throws IOException {
        client().performRequest(new Request("DELETE", indexName()));
    }

    private String indexName() {
        return getTestName().toLowerCase(Locale.ROOT);
    }

    private String docPath() {
        return indexName() + "/test/1";
    }

    public void testIndexAndThenClose() throws Exception {
        closeWhileListenerEngaged(start("PUT", "", "{\"test\":\"test\"}"));
    }

    public void testUpdateAndThenClose() throws Exception {
        Request request = new Request("PUT", docPath());
        request.setJsonEntity("{\"test\":\"test\"}");
        client().performRequest(request);
        closeWhileListenerEngaged(start("POST", "/_update", "{\"doc\":{\"name\":\"test\"}}"));
    }

    public void testDeleteAndThenClose() throws Exception {
        Request request = new Request("PUT", docPath());
        request.setJsonEntity("{\"test\":\"test\"}");
        client().performRequest(request);
        closeWhileListenerEngaged(start("DELETE", "", null));
    }

    private void closeWhileListenerEngaged(ActionFuture<String> future) throws Exception {
        // Wait for the refresh listener to start waiting
        assertBusy(() -> {
            Map<String, Object> stats;
            try {
                stats = entityAsMap(client().performRequest(new Request("GET", indexName() + "/_stats/refresh")));
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
        client().performRequest(new Request("POST", indexName() + "/_close"));

        // The request shouldn't fail. It certainly shouldn't hang.
        future.get();
    }

    private ActionFuture<String> start(String method, String path, String body) {
        PlainActionFuture<String> future = new PlainActionFuture<>();
        Request request = new Request(method, docPath() + path);
        request.addParameter("refresh", "wait_for");
        request.addParameter("error_trace", "");
        request.setJsonEntity(body);
        client().performRequestAsync(request, new ResponseListener() {
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
