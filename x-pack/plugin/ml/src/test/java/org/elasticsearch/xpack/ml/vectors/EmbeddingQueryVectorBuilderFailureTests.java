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
import org.elasticsearch.inference.DataType;
import org.elasticsearch.inference.WeightedToken;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.EmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResults;
import org.elasticsearch.xpack.core.ml.vectors.EmbeddingQueryVectorBuilder;

import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class EmbeddingQueryVectorBuilderFailureTests extends ESTestCase {
    private static class MockClient extends NoOpClient {
        private final Supplier<InferenceAction.Response> responseSupplier;

        MockClient(ThreadPool threadPool, Supplier<InferenceAction.Response> responseSupplier) {
            super(threadPool);
            this.responseSupplier = responseSupplier;
        }

        @Override
        @SuppressWarnings("unchecked")
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            listener.onResponse((Response) responseSupplier.get());
        }
    }

    public void testReceivingSparseEmbeddingResults_ThrowsIllegalStateException() {
        var queryVectorBuilder = createTestInstance();
        try (var threadPool = createThreadPool()) {
            var sparseResults = new SparseEmbeddingResults(
                List.of(new SparseEmbeddingResults.Embedding(List.of(new WeightedToken("token", 0.1f)), false))
            );

            final var client = new MockClient(threadPool, () -> new InferenceAction.Response(sparseResults));
            PlainActionFuture<float[]> future = new PlainActionFuture<>();
            queryVectorBuilder.buildVector(client, future);

            var exception = expectThrows(IllegalStateException.class, () -> future.actionGet(TEST_REQUEST_TIMEOUT));
            assertThat(
                exception.getMessage(),
                equalTo(
                    "expected a result of type ["
                        + EmbeddingFloatResults.class.getSimpleName()
                        + "], received ["
                        + sparseResults.getClass().getSimpleName()
                        + "]"
                )
            );
        }
    }

    private static EmbeddingQueryVectorBuilder createTestInstance() {
        return new EmbeddingQueryVectorBuilder(randomAlphaOfLength(4), randomFrom(DataType.values()), null, randomAlphaOfLength(4), null);
    }
}
