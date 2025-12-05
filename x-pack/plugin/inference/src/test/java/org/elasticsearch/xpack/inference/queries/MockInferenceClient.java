/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResults;
import org.elasticsearch.xpack.core.ml.action.CoordinatedInferenceAction;
import org.elasticsearch.xpack.core.ml.action.InferModelAction;
import org.elasticsearch.xpack.core.ml.inference.results.MlDenseEmbeddingResults;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;

import java.util.List;
import java.util.Map;

public class MockInferenceClient extends NoOpClient {
    private final MockInferenceGenerator inferenceGenerator;

    protected MockInferenceClient(ThreadPool threadPool, Map<String, MinimalServiceSettings> inferenceEndpoints) {
        super(threadPool);
        this.inferenceGenerator = new MockInferenceGenerator(inferenceEndpoints);
    }

    @Override
    protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
        ActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        if (action instanceof InferenceAction && request instanceof InferenceAction.Request inferenceRequest) {
            @SuppressWarnings("unchecked")
            ActionListener<InferenceAction.Response> inferenceListener = (ActionListener<InferenceAction.Response>) listener;

            String inferenceId = inferenceRequest.getInferenceEntityId();
            String input = inferenceRequest.getInput().getFirst();
            try {
                InferenceServiceResults inferenceServiceResults;
                InferenceResults inferenceResults = inferenceGenerator.generate(inferenceId, input);
                if (inferenceResults instanceof TextExpansionResults textExpansionResults) {
                    inferenceServiceResults = SparseEmbeddingResults.of(List.of(textExpansionResults));
                } else if (inferenceResults instanceof MlDenseEmbeddingResults mlDenseEmbeddingResults) {
                    inferenceServiceResults = DenseEmbeddingFloatResults.of(List.of(mlDenseEmbeddingResults));
                } else {
                    throw new IllegalStateException("Unexpected inference results type [" + inferenceResults.getWriteableName() + "]");
                }

                inferenceListener.onResponse(new InferenceAction.Response(inferenceServiceResults));
            } catch (Exception e) {
                inferenceListener.onFailure(e);
            }
        } else if (action instanceof CoordinatedInferenceAction && request instanceof CoordinatedInferenceAction.Request inferenceRequest) {
            @SuppressWarnings("unchecked")
            ActionListener<InferModelAction.Response> inferenceListener = (ActionListener<InferModelAction.Response>) listener;

            String inferenceId = inferenceRequest.getModelId();
            String input = inferenceRequest.getInputs().getFirst();
            try {
                InferenceResults inferenceResults = inferenceGenerator.generate(inferenceId, input);
                inferenceListener.onResponse(new InferModelAction.Response(List.of(inferenceResults), inferenceId, true));
            } catch (Exception e) {
                inferenceListener.onFailure(e);
            }
        } else {
            super.doExecute(action, request, listener);
        }
    }
}
