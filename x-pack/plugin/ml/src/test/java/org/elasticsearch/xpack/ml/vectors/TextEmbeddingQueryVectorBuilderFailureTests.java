/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.vectors;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.WeightedToken;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.action.InferModelAction;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;
import org.elasticsearch.xpack.core.ml.vectors.TextEmbeddingQueryVectorBuilder;

import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class TextEmbeddingQueryVectorBuilderFailureTests extends ESTestCase {

    private static class AssertingClient extends NoOpClient {
        private final TextEmbeddingQueryVectorBuilder queryVectorBuilder;

        AssertingClient(ThreadPool threadPool, TextEmbeddingQueryVectorBuilder queryVectorBuilder) {
            super(threadPool);
            this.queryVectorBuilder = queryVectorBuilder;
        }

        @Override
        @SuppressWarnings("unchecked")
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            listener.onResponse((Response) createResponse(queryVectorBuilder.getModelId()));
        }
    }

    public void testReceiving_TextExpansionResults_ThrowsBadRequestException() {
        var queryVectorBuilder = createTestInstance();

        try (var threadPool = createThreadPool()) {
            final var client = new AssertingClient(threadPool, queryVectorBuilder);
            PlainActionFuture<float[]> future = new PlainActionFuture<>();
            queryVectorBuilder.buildVector(client, future);

            var exception = expectThrows(IllegalArgumentException.class, () -> future.actionGet(TimeValue.timeValueSeconds(30)));
            assertThat(
                exception.getMessage(),
                containsString("expected a result of type [text_embedding_result] received [text_expansion_result]")
            );
        }
    }

    private static ActionResponse createResponse(String modelId) {
        return new InferModelAction.Response(
            List.of(new TextExpansionResults("foo", List.of(new WeightedToken("toke", 0.1f)), randomBoolean())),
            modelId,
            true
        );
    }

    private static TextEmbeddingQueryVectorBuilder createTestInstance() {
        return new TextEmbeddingQueryVectorBuilder(randomAlphaOfLength(4), randomAlphaOfLength(4));
    }
}
