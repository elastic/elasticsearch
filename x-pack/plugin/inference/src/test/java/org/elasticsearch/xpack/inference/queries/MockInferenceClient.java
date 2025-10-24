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
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.WeightedToken;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResults;
import org.elasticsearch.xpack.core.ml.action.CoordinatedInferenceAction;
import org.elasticsearch.xpack.core.ml.action.InferModelAction;
import org.elasticsearch.xpack.core.ml.inference.results.MlDenseEmbeddingResults;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

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
            String input = inferenceRequest.getInput().getFirst();
            try {
                InferenceServiceResults inferenceServiceResults;
                InferenceResults inferenceResults = generateInferenceResults(inferenceId, input);
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
                InferenceResults inferenceResults = generateInferenceResults(inferenceId, input);
                inferenceListener.onResponse(new InferModelAction.Response(List.of(inferenceResults), inferenceId, true));
            } catch (Exception e) {
                inferenceListener.onFailure(e);
            }
        } else {
            super.doExecute(action, request, listener);
        }
    }

    private InferenceResults generateInferenceResults(String inferenceId, String input) {
        MinimalServiceSettings inferenceEndpointSettings = inferenceEndpoints.get(inferenceId);

        InferenceResults inferenceResults;
        if (inferenceEndpointSettings == null) {
            throw new IllegalArgumentException("Inference endpoint [" + inferenceId + "] does not exist");
        } else if (inferenceEndpointSettings.taskType() == TaskType.SPARSE_EMBEDDING) {
            inferenceResults = generateTextExpansionResults(input);
        } else if (inferenceEndpointSettings.taskType() == TaskType.TEXT_EMBEDDING) {
            inferenceResults = generateTextEmbeddingResults(inferenceEndpointSettings);
        } else {
            throw new IllegalArgumentException(
                "Invalid task type [" + inferenceEndpointSettings.taskType() + "] for inference endpoint [" + inferenceId + "]"
            );
        }

        return inferenceResults;
    }

    /**
     * Generate text expansion results. Use a static token weight so that the results are deterministic for the same query.
     */
    private static InferenceResults generateTextExpansionResults(String input) {
        List<WeightedToken> weightedTokens = Arrays.stream(input.split("\\s+")).map(token -> new WeightedToken(token, 1.0f)).toList();
        return new TextExpansionResults(DEFAULT_RESULTS_FIELD, weightedTokens, false);
    }

    /**
     * Generate text embedding results. Use static embedding values so that the results are deterministic for the same dimension count.
     */
    private static InferenceResults generateTextEmbeddingResults(MinimalServiceSettings settings) {
        assert settings.dimensions() != null && settings.elementType() != null;

        int embeddingSize = settings.dimensions();
        if (settings.elementType() == DenseVectorFieldMapper.ElementType.BIT) {
            embeddingSize /= 8;
        }

        double[] embedding = new double[embeddingSize];
        Arrays.fill(embedding, Byte.MIN_VALUE);  // Always use a byte value so that the embedding is valid regardless of the element type

        return new MlDenseEmbeddingResults(DEFAULT_RESULTS_FIELD, embedding, false);
    }
}
