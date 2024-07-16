/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googleaistudio;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkedInferenceServiceResults;
import org.elasticsearch.inference.ChunkingOptions;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.inference.chunking.EmbeddingRequestChunker;
import org.elasticsearch.xpack.inference.external.action.googleaistudio.GoogleAiStudioActionCreator;
import org.elasticsearch.xpack.inference.external.http.sender.DocumentsOnlyInput;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.SenderService;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.googleaistudio.completion.GoogleAiStudioCompletionModel;
import org.elasticsearch.xpack.inference.services.googleaistudio.embeddings.GoogleAiStudioEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.googleaistudio.embeddings.GoogleAiStudioEmbeddingsServiceSettings;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.createInvalidModelException;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.parsePersistedConfigErrorMsg;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrDefaultEmpty;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrThrowIfNull;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.throwIfNotEmptyMap;
import static org.elasticsearch.xpack.inference.services.googleaistudio.GoogleAiStudioServiceFields.EMBEDDING_MAX_BATCH_SIZE;

public class GoogleAiStudioService extends SenderService {

    public static final String NAME = "googleaistudio";

    public GoogleAiStudioService(HttpRequestSender.Factory factory, ServiceComponents serviceComponents) {
        super(factory, serviceComponents);
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void parseRequestConfig(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> config,
        Set<String> platformArchitectures,
        ActionListener<Model> parsedModelListener
    ) {
        try {
            Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
            Map<String, Object> taskSettingsMap = removeFromMapOrDefaultEmpty(config, ModelConfigurations.TASK_SETTINGS);

            GoogleAiStudioModel model = createModel(
                inferenceEntityId,
                taskType,
                serviceSettingsMap,
                taskSettingsMap,
                serviceSettingsMap,
                TaskType.unsupportedTaskTypeErrorMsg(taskType, NAME),
                ConfigurationParseContext.REQUEST
            );

            throwIfNotEmptyMap(config, NAME);
            throwIfNotEmptyMap(serviceSettingsMap, NAME);
            throwIfNotEmptyMap(taskSettingsMap, NAME);

            parsedModelListener.onResponse(model);
        } catch (Exception e) {
            parsedModelListener.onFailure(e);
        }

    }

    private static GoogleAiStudioModel createModel(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        @Nullable Map<String, Object> secretSettings,
        String failureMessage,
        ConfigurationParseContext context
    ) {
        return switch (taskType) {
            case COMPLETION -> new GoogleAiStudioCompletionModel(
                inferenceEntityId,
                taskType,
                NAME,
                serviceSettings,
                taskSettings,
                secretSettings,
                context
            );
            case TEXT_EMBEDDING -> new GoogleAiStudioEmbeddingsModel(
                inferenceEntityId,
                taskType,
                NAME,
                serviceSettings,
                taskSettings,
                secretSettings,
                context
            );
            default -> throw new ElasticsearchStatusException(failureMessage, RestStatus.BAD_REQUEST);
        };
    }

    @Override
    public GoogleAiStudioModel parsePersistedConfigWithSecrets(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> config,
        Map<String, Object> secrets
    ) {
        Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
        Map<String, Object> taskSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.TASK_SETTINGS);
        Map<String, Object> secretSettingsMap = removeFromMapOrDefaultEmpty(secrets, ModelSecrets.SECRET_SETTINGS);

        return createModelFromPersistent(
            inferenceEntityId,
            taskType,
            serviceSettingsMap,
            taskSettingsMap,
            secretSettingsMap,
            parsePersistedConfigErrorMsg(inferenceEntityId, NAME)
        );
    }

    private static GoogleAiStudioModel createModelFromPersistent(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        Map<String, Object> secretSettings,
        String failureMessage
    ) {
        return createModel(
            inferenceEntityId,
            taskType,
            serviceSettings,
            taskSettings,
            secretSettings,
            failureMessage,
            ConfigurationParseContext.PERSISTENT
        );
    }

    @Override
    public Model parsePersistedConfig(String inferenceEntityId, TaskType taskType, Map<String, Object> config) {
        Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
        Map<String, Object> taskSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.TASK_SETTINGS);

        return createModelFromPersistent(
            inferenceEntityId,
            taskType,
            serviceSettingsMap,
            taskSettingsMap,
            null,
            parsePersistedConfigErrorMsg(inferenceEntityId, NAME)
        );
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ML_INFERENCE_GOOGLE_AI_STUDIO_COMPLETION_ADDED;
    }

    @Override
    public void checkModelConfig(Model model, ActionListener<Model> listener) {
        if (model instanceof GoogleAiStudioEmbeddingsModel embeddingsModel) {
            ServiceUtils.getEmbeddingSize(
                model,
                this,
                listener.delegateFailureAndWrap((l, size) -> l.onResponse(updateModelWithEmbeddingDetails(embeddingsModel, size)))
            );
        } else {
            listener.onResponse(model);
        }
    }

    private GoogleAiStudioEmbeddingsModel updateModelWithEmbeddingDetails(GoogleAiStudioEmbeddingsModel model, int embeddingSize) {
        var similarityFromModel = model.getServiceSettings().similarity();
        var similarityToUse = similarityFromModel == null ? SimilarityMeasure.DOT_PRODUCT : similarityFromModel;

        GoogleAiStudioEmbeddingsServiceSettings serviceSettings = new GoogleAiStudioEmbeddingsServiceSettings(
            model.getServiceSettings().modelId(),
            model.getServiceSettings().maxInputTokens(),
            embeddingSize,
            similarityToUse,
            model.getServiceSettings().rateLimitSettings()
        );

        return new GoogleAiStudioEmbeddingsModel(model, serviceSettings);
    }

    @Override
    protected void doInfer(
        Model model,
        List<String> input,
        Map<String, Object> taskSettings,
        InputType inputType,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        if (model instanceof GoogleAiStudioModel == false) {
            listener.onFailure(createInvalidModelException(model));
            return;
        }

        GoogleAiStudioModel googleAiStudioModel = (GoogleAiStudioModel) model;
        var actionCreator = new GoogleAiStudioActionCreator(getSender(), getServiceComponents());

        var action = googleAiStudioModel.accept(actionCreator, taskSettings, inputType);
        action.execute(new DocumentsOnlyInput(input), timeout, listener);
    }

    @Override
    protected void doInfer(
        Model model,
        String query,
        List<String> input,
        Map<String, Object> taskSettings,
        InputType inputType,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        throw new UnsupportedOperationException("Query input not supported for Google AI Studio");
    }

    @Override
    protected void doChunkedInfer(
        Model model,
        String query,
        List<String> input,
        Map<String, Object> taskSettings,
        InputType inputType,
        ChunkingOptions chunkingOptions,
        TimeValue timeout,
        ActionListener<List<ChunkedInferenceServiceResults>> listener
    ) {
        GoogleAiStudioModel googleAiStudioModel = (GoogleAiStudioModel) model;
        var actionCreator = new GoogleAiStudioActionCreator(getSender(), getServiceComponents());

        var batchedRequests = new EmbeddingRequestChunker(input, EMBEDDING_MAX_BATCH_SIZE, EmbeddingRequestChunker.EmbeddingType.FLOAT)
            .batchRequestsWithListeners(listener);
        for (var request : batchedRequests) {
            var action = googleAiStudioModel.accept(actionCreator, taskSettings, inputType);
            action.execute(new DocumentsOnlyInput(request.batch().inputs()), timeout, request.listener());
        }
    }
}
