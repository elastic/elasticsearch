/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.rest;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Tests that wait for refresh is fired if the index is closed.
 */
public class WaitForRefreshAndCloseIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local().build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Before
    public void setupIndex() throws IOException {
        Request request = new Request("PUT", "/test");
        request.setJsonEntity("{\"settings\":{\"refresh_interval\":-1}}");
        client().performRequest(request);
    }

    @After
    public void cleanupIndex() throws IOException {
        client().performRequest(new Request("DELETE", "/test"));
    }

    private String docPath() {
        return "test/_doc/1";
    }

    public void testIndexAndThenClose() throws Exception {
        Request request = new Request("PUT", docPath());
        request.setJsonEntity("{\"test\":\"test\"}");
        closeWhileListenerEngaged(start(request));
    }

    public void testUpdateAndThenClose() throws Exception {
        Request createDoc = new Request("PUT", docPath());
        createDoc.setJsonEntity("{\"test\":\"test\"}");
        client().performRequest(createDoc);
        Request updateDoc = new Request("POST", "test/_update/1");
        updateDoc.setJsonEntity("{\"doc\":{\"name\":\"test\"}}");
        closeWhileListenerEngaged(start(updateDoc));
    }

    public void testDeleteAndThenClose() throws Exception {
        Request request = new Request("PUT", docPath());
        request.setJsonEntity("{\"test\":\"test\"}");
        client().performRequest(request);
        closeWhileListenerEngaged(start(new Request("DELETE", docPath())));
    }

    private void closeWhileListenerEngaged(ActionFuture<String> future) throws Exception {
        // Wait for the refresh listener to start waiting
        assertBusy(() -> {
            Map<String, Object> stats;
            try {
                stats = entityAsMap(client().performRequest(new Request("GET", "/test/_stats/refresh")));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            Map<?, ?> indices = (Map<?, ?>) stats.get("indices");
            Map<?, ?> theIndex = (Map<?, ?>) indices.get("test");
            Map<?, ?> total = (Map<?, ?>) theIndex.get("total");
            Map<?, ?> refresh = (Map<?, ?>) total.get("refresh");
            int listeners = (Integer) refresh.get("listeners");
            assertEquals(1, listeners);
        }, 30L, TimeUnit.SECONDS);

        // Close the index. That should flush the listener.
        client().performRequest(new Request("POST", "/test/_close"));

        /*
         * The request may fail, but we really, really, really want to make
         * sure that it doesn't time out.
         */
        try {
            future.get(1, TimeUnit.MINUTES);
        } catch (ExecutionException ee) {
            /*
             * If it *does* fail it should fail with a FORBIDDEN error because
             * it attempts to take an action on a closed index. Again, it'd be
             * nice if all requests waiting for refresh came back even though
             * the index is closed and most do, but sometimes they bump into
             * the index being closed. At least they don't hang forever. That'd
             * be a nightmare.
             */
            assertThat(ee.getCause(), instanceOf(ResponseException.class));
            ResponseException re = (ResponseException) ee.getCause();
            assertEquals(403, re.getResponse().getStatusLine().getStatusCode());
            assertThat(EntityUtils.toString(re.getResponse().getEntity()), containsString("FORBIDDEN/4/index closed"));
        }
    }

    private ActionFuture<String> start(Request request) {
        PlainActionFuture<String> future = new PlainActionFuture<>();
        request.addParameter("refresh", "wait_for");
        request.addParameter("error_trace", "");
        client().performRequestAsync(request, new ResponseListener() {
            @Override
            public void onSuccess(Response response) {
                ActionListener.completeWith(future, () -> EntityUtils.toString(response.getEntity()));
            }

            @Override
            public void onFailure(Exception exception) {
                future.onFailure(exception);
            }
        });
        return future;
    }
}
