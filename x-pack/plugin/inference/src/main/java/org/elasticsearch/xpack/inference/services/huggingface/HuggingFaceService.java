/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkedInferenceServiceResults;
import org.elasticsearch.inference.ChunkingOptions;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.inference.chunking.EmbeddingRequestChunker;
import org.elasticsearch.xpack.inference.external.action.huggingface.HuggingFaceActionCreator;
import org.elasticsearch.xpack.inference.external.http.sender.DocumentsOnlyInput;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.huggingface.elser.HuggingFaceElserModel;
import org.elasticsearch.xpack.inference.services.huggingface.embeddings.HuggingFaceEmbeddingsModel;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.createInvalidModelException;

public class HuggingFaceService extends HuggingFaceBaseService {
    public static final String NAME = "hugging_face";

    public HuggingFaceService(HttpRequestSender.Factory factory, ServiceComponents serviceComponents) {
        super(factory, serviceComponents);
    }

    @Override
    protected HuggingFaceModel createModel(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        @Nullable Map<String, Object> secretSettings,
        String failureMessage,
        ConfigurationParseContext context
    ) {
        return switch (taskType) {
            case TEXT_EMBEDDING -> new HuggingFaceEmbeddingsModel(
                inferenceEntityId,
                taskType,
                NAME,
                serviceSettings,
                secretSettings,
                context
            );
            case SPARSE_EMBEDDING -> new HuggingFaceElserModel(inferenceEntityId, taskType, NAME, serviceSettings, secretSettings, context);
            default -> throw new ElasticsearchStatusException(failureMessage, RestStatus.BAD_REQUEST);
        };
    }

    @Override
    public void checkModelConfig(Model model, ActionListener<Model> listener) {
        if (model instanceof HuggingFaceEmbeddingsModel embeddingsModel) {
            ServiceUtils.getEmbeddingSize(
                model,
                this,
                listener.delegateFailureAndWrap((l, size) -> l.onResponse(updateModelWithEmbeddingDetails(embeddingsModel, size)))
            );
        } else {
            listener.onResponse(model);
        }
    }

    private static HuggingFaceEmbeddingsModel updateModelWithEmbeddingDetails(HuggingFaceEmbeddingsModel model, int embeddingSize) {
        // default to cosine similarity
        var similarity = model.getServiceSettings().similarity() == null
            ? SimilarityMeasure.COSINE
            : model.getServiceSettings().similarity();

        var serviceSettings = new HuggingFaceServiceSettings(
            model.getServiceSettings().uri(),
            similarity,
            embeddingSize,
            model.getTokenLimit(),
            model.getServiceSettings().rateLimitSettings()
        );

        return new HuggingFaceEmbeddingsModel(model, serviceSettings);
    }

    @Override
    protected void doChunkedInfer(
        Model model,
        @Nullable String query,
        List<String> input,
        Map<String, Object> taskSettings,
        InputType inputType,
        ChunkingOptions chunkingOptions,
        TimeValue timeout,
        ActionListener<List<ChunkedInferenceServiceResults>> listener
    ) {
        if (model instanceof HuggingFaceModel == false) {
            listener.onFailure(createInvalidModelException(model));
            return;
        }

        var huggingFaceModel = (HuggingFaceModel) model;
        var actionCreator = new HuggingFaceActionCreator(getSender(), getServiceComponents());

        var batchedRequests = new EmbeddingRequestChunker(input, EMBEDDING_MAX_BATCH_SIZE, EmbeddingRequestChunker.EmbeddingType.FLOAT)
            .batchRequestsWithListeners(listener);
        for (var request : batchedRequests) {
            var action = huggingFaceModel.accept(actionCreator);
            action.execute(new DocumentsOnlyInput(request.batch().inputs()), timeout, request.listener());
        }
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ML_INFERENCE_RATE_LIMIT_SETTINGS_ADDED;
    }
}
