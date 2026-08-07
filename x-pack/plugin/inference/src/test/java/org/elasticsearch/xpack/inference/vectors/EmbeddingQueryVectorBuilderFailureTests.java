/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.vectors;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.inference.InferenceStringGroupTests;
import org.elasticsearch.inference.WeightedToken;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.EmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.GenericDenseEmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResults;
import org.elasticsearch.xpack.core.ml.inference.results.MlDenseEmbeddingResults;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;
import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class EmbeddingQueryVectorBuilderFailureTests extends ESTestCase {
    private ThreadPool threadPool;

    @Before
    public void setupThreadPool() {
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void shutdownThreadPool() {
        threadPool.shutdownNow();
    }

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

    public void testEmptyResults_ThrowsIllegalStateException() {
        var queryVectorBuilder = createTestInstance();
        var emptyResults = new GenericDenseEmbeddingFloatResults(List.of());

        final var client = new MockClient(threadPool, () -> new InferenceAction.Response(emptyResults));
        PlainActionFuture<float[]> future = new PlainActionFuture<>();
        queryVectorBuilder.buildVector(client, future);

        var exception = expectThrows(IllegalStateException.class, () -> future.actionGet(TEST_REQUEST_TIMEOUT));
        assertThat(exception.getMessage(), equalTo("the query embedding response contains no results"));
    }

    public void testMultipleResults_ThrowsIllegalStateException() {
        var queryVectorBuilder = createTestInstance();
        var multipleResults = new GenericDenseEmbeddingFloatResults(
            List.of(
                new EmbeddingFloatResults.Embedding(new float[] { 1.0f }),
                new EmbeddingFloatResults.Embedding(new float[] { 2.0f }),
                new EmbeddingFloatResults.Embedding(new float[] { 3.0f })
            )
        );

        final var client = new MockClient(threadPool, () -> new InferenceAction.Response(multipleResults));
        PlainActionFuture<float[]> future = new PlainActionFuture<>();
        queryVectorBuilder.buildVector(client, future);

        var exception = expectThrows(IllegalStateException.class, () -> future.actionGet(TEST_REQUEST_TIMEOUT));
        assertThat(exception.getMessage(), equalTo("the query embedding response contains 3 results"));
    }

    public void testReceivingSparseEmbeddingResults_ThrowsIllegalStateException() {
        var queryVectorBuilder = createTestInstance();
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
                "expected inference results to be of type ["
                    + MlDenseEmbeddingResults.NAME
                    + "], received ["
                    + TextExpansionResults.NAME
                    + "]"
            )
        );
    }

    private static EmbeddingQueryVectorBuilder createTestInstance() {
        return new EmbeddingQueryVectorBuilder(randomAlphaOfLength(4), InferenceStringGroupTests.createRandom(), null);
    }
}
