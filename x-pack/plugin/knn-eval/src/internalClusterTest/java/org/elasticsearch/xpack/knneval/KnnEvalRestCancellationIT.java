/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.knneval;

import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.action.support.CancellableActionTestPlugin;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.root.MainRestPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.netty4.Netty4Plugin;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.action.support.ActionTestUtils.wrapAsRestResponseListener;
import static org.elasticsearch.test.TaskAssertions.assertAllTasksHaveFinished;

/** A synchronous evaluation can run for minutes, so closing the HTTP connection must cancel it. */
public class KnnEvalRestCancellationIT extends ESIntegTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(NetworkModule.TRANSPORT_TYPE_KEY, Netty4Plugin.NETTY_TRANSPORT_NAME)
            .put(NetworkModule.HTTP_TYPE_KEY, Netty4Plugin.NETTY_HTTP_TRANSPORT_NAME)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(getTestTransportPlugin(), MainRestPlugin.class, CancellableActionTestPlugin.class, KnnEvalPlugin.class);
    }

    public void testClosingTheConnectionCancelsTheEvaluation() throws Exception {
        final var node = internalCluster().getRandomNodeName();
        final var request = new Request(HttpPost.METHOD_NAME, "/test/" + RestKnnEvalAction.ENDPOINT);
        request.setJsonEntity("""
            {
              "field": "emb",
              "k": 1,
              "queries": [ { "id": "q", "query_vector": [ 1.0 ] } ],
              "knn_settings": [ { "visit_percentage": 1 } ]
            }""");

        try (
            var restClient = createRestClient(node);
            var capturingAction = CancellableActionTestPlugin.capturingActionOnNode(KnnEvalPlugin.KNN_EVAL_ACTION.name(), node)
        ) {
            final var responseFuture = new PlainActionFuture<Response>();
            final var restInvocation = restClient.performRequestAsync(request, wrapAsRestResponseListener(responseFuture));
            capturingAction.captureAndCancel(restInvocation::cancel);
            expectThrows(ExecutionException.class, CancellationException.class, () -> responseFuture.get(10, TimeUnit.SECONDS));
        }
        assertAllTasksHaveFinished(KnnEvalPlugin.KNN_EVAL_ACTION.name());
    }
}
