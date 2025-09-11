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
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.WeightedToken;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResults;
import org.elasticsearch.xpack.core.ml.inference.results.ErrorInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.MlTextEmbeddingResults;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.ESTestCase.randomByte;
import static org.elasticsearch.test.ESTestCase.randomFloatBetween;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig.DEFAULT_RESULTS_FIELD;

public class MockInferenceClient extends NoOpClient {
    private final Map<String, MinimalServiceSettings> inferenceEndpoints;

    protected MockInferenceClient(ThreadPool threadPool, Map<String, MinimalServiceSettings> inferenceEndpoints) {
        super(threadPool);
        this.inferenceEndpoints = inferenceEndpoints;
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
            MinimalServiceSettings inferenceEndpointSettings = inferenceEndpoints.get(inferenceId);

            InferenceServiceResults inferenceServiceResults;
            if (inferenceEndpointSettings == null) {
                inferenceServiceResults = TextEmbeddingFloatResults.of(
                    List.of(
                        new ErrorInferenceResults(new IllegalArgumentException("Inference endpoint [" + inferenceId + "] does not exist"))
                    )
                );
            } else if (inferenceEndpointSettings.taskType() == TaskType.SPARSE_EMBEDDING) {
                inferenceServiceResults = generateRandomSparseEmbeddingResults(inferenceRequest);
            } else if (inferenceEndpointSettings.taskType() == TaskType.TEXT_EMBEDDING) {
                inferenceServiceResults = generateRandomTextEmbeddingResults(inferenceEndpointSettings);
            } else {
                inferenceServiceResults = TextEmbeddingFloatResults.of(
                    List.of(
                        new ErrorInferenceResults(
                            new IllegalArgumentException(
                                "Invalid task type ["
                                    + inferenceEndpointSettings.taskType()
                                    + "] for inference endpoint ["
                                    + inferenceId
                                    + "]"
                            )
                        )
                    )
                );
            }

            inferenceListener.onResponse(new InferenceAction.Response(inferenceServiceResults));
        } else {
            super.doExecute(action, request, listener);
        }
    }

    private static SparseEmbeddingResults generateRandomSparseEmbeddingResults(InferenceAction.Request request) {
        String query = request.getInput().getFirst();

        List<WeightedToken> weightedTokens = Arrays.stream(query.split("\\s+"))
            .map(token -> new WeightedToken(token, randomFloatBetween(0.0f, 1.0f, false)))
            .toList();

        TextExpansionResults textExpansionResults = new TextExpansionResults(DEFAULT_RESULTS_FIELD, weightedTokens, false);
        return SparseEmbeddingResults.of(List.of(textExpansionResults));
    }

    private static TextEmbeddingFloatResults generateRandomTextEmbeddingResults(MinimalServiceSettings settings) {
        assert settings.dimensions() != null && settings.elementType() != null;

        int embeddingSize = settings.dimensions();
        if (settings.elementType() == DenseVectorFieldMapper.ElementType.BIT) {
            embeddingSize /= 8;
        }

        double[] embedding = new double[embeddingSize];
        for (int i = 0; i < embedding.length; i++) {
            // Always use a byte value so that the embedding is valid regardless of the element type
            embedding[i] = randomByte();
        }

        MlTextEmbeddingResults mlTextEmbeddingResults = new MlTextEmbeddingResults(DEFAULT_RESULTS_FIELD, embedding, false);
        return TextEmbeddingFloatResults.of(List.of(mlTextEmbeddingResults));
    }
}
