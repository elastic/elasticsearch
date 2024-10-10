/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface.elser;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkedInferenceServiceResults;
import org.elasticsearch.inference.ChunkingOptions;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.inference.results.ErrorChunkedInferenceResults;
import org.elasticsearch.xpack.core.inference.results.InferenceChunkedSparseEmbeddingResults;
import org.elasticsearch.xpack.core.inference.results.InferenceChunkedTextEmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.InferenceTextEmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResults;
import org.elasticsearch.xpack.core.ml.inference.results.ErrorInferenceResults;
import org.elasticsearch.xpack.inference.external.http.sender.DocumentsOnlyInput;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.huggingface.HuggingFaceBaseService;
import org.elasticsearch.xpack.inference.services.huggingface.HuggingFaceModel;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.inference.results.ResultUtils.createInvalidChunkedResultException;

public class HuggingFaceElserService extends HuggingFaceBaseService {
    public static final String NAME = "hugging_face_elser";

    public HuggingFaceElserService(HttpRequestSender.Factory factory, ServiceComponents serviceComponents) {
        super(factory, serviceComponents);
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    protected HuggingFaceModel createModel(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        ChunkingSettings chunkingSettings,
        @Nullable Map<String, Object> secretSettings,
        String failureMessage,
        ConfigurationParseContext context
    ) {
        return switch (taskType) {
            case SPARSE_EMBEDDING -> new HuggingFaceElserModel(inferenceEntityId, taskType, NAME, serviceSettings, secretSettings, context);
            default -> throw new ElasticsearchStatusException(failureMessage, RestStatus.BAD_REQUEST);
        };
    }

    @Override
    protected void doChunkedInfer(
        Model model,
        DocumentsOnlyInput inputs,
        Map<String, Object> taskSettings,
        InputType inputType,
        ChunkingOptions chunkingOptions,
        TimeValue timeout,
        ActionListener<List<ChunkedInferenceServiceResults>> listener
    ) {
        ActionListener<InferenceServiceResults> inferListener = listener.delegateFailureAndWrap(
            (delegate, response) -> delegate.onResponse(translateToChunkedResults(inputs, response))
        );

        // TODO chunking sparse embeddings not implemented
        doInfer(model, inputs, taskSettings, inputType, timeout, inferListener);
    }

    private static List<ChunkedInferenceServiceResults> translateToChunkedResults(
        DocumentsOnlyInput inputs,
        InferenceServiceResults inferenceResults
    ) {
        if (inferenceResults instanceof InferenceTextEmbeddingFloatResults textEmbeddingResults) {
            return InferenceChunkedTextEmbeddingFloatResults.listOf(inputs.getInputs(), textEmbeddingResults);
        } else if (inferenceResults instanceof SparseEmbeddingResults sparseEmbeddingResults) {
            return InferenceChunkedSparseEmbeddingResults.listOf(inputs.getInputs(), sparseEmbeddingResults);
        } else if (inferenceResults instanceof ErrorInferenceResults error) {
            return List.of(new ErrorChunkedInferenceResults(error.getException()));
        } else {
            String expectedClasses = Strings.format(
                "One of [%s,%s]",
                InferenceTextEmbeddingFloatResults.class.getSimpleName(),
                SparseEmbeddingResults.class.getSimpleName()
            );
            throw createInvalidChunkedResultException(expectedClasses, inferenceResults.getWriteableName());
        }
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_12_0;
    }
}
