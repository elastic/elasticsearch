/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.fireworksai;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkInferenceInput;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.InferenceServiceConfiguration;
import org.elasticsearch.inference.InferenceServiceExtension;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.RerankingInferenceService;
import org.elasticsearch.inference.SettingsConfiguration;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.configuration.SettingsConfigurationFieldType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsBuilder;
import org.elasticsearch.xpack.core.inference.chunking.EmbeddingRequestChunker;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.SenderService;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.fireworksai.action.FireworksAiActionCreator;
import org.elasticsearch.xpack.inference.services.fireworksai.embeddings.FireworksAiEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.fireworksai.rerank.FireworksAiRerankModel;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createInvalidModelException;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createInvalidTaskTypeException;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMap;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrDefaultEmpty;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrThrowIfNull;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.throwIfNotEmptyMap;

/**
 * FireworksAI inference service for embeddings and reranking tasks.
 * This service uses the FireworksAI REST API to perform text embeddings and document reranking.
 */
public class FireworksAiService extends SenderService implements RerankingInferenceService {
    public static final String NAME = "fireworksai";
    private static final String SERVICE_NAME = "FireworksAI";

    private static final TransportVersion FIREWORKS_AI_SERVICE = TransportVersion.fromName("fireworks_ai_service");

    private static final EnumSet<TaskType> SUPPORTED_TASK_TYPES = EnumSet.of(TaskType.TEXT_EMBEDDING, TaskType.RERANK);

    // FireworksAI embeddings max batch size (adjust based on actual API limits)
    private static final int EMBEDDING_MAX_BATCH_SIZE = 2048;

    public FireworksAiService(
        HttpRequestSender.Factory factory,
        ServiceComponents serviceComponents,
        InferenceServiceExtension.InferenceServiceFactoryContext context
    ) {
        this(factory, serviceComponents, context.clusterService());
    }

    public FireworksAiService(HttpRequestSender.Factory factory, ServiceComponents serviceComponents, ClusterService clusterService) {
        super(factory, serviceComponents, clusterService);
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public EnumSet<TaskType> supportedTaskTypes() {
        return SUPPORTED_TASK_TYPES;
    }

    @Override
    public void parseRequestConfig(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> config,
        ActionListener<Model> parsedModelListener
    ) {
        try {
            Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
            Map<String, Object> taskSettingsMap = removeFromMapOrDefaultEmpty(config, ModelConfigurations.TASK_SETTINGS);

            ChunkingSettings chunkingSettings = null;
            if (TaskType.TEXT_EMBEDDING.equals(taskType)) {
                chunkingSettings = ChunkingSettingsBuilder.fromMap(
                    removeFromMapOrDefaultEmpty(config, ModelConfigurations.CHUNKING_SETTINGS)
                );
            }

            FireworksAiModel model = createModel(
                inferenceEntityId,
                taskType,
                serviceSettingsMap,
                taskSettingsMap,
                chunkingSettings,
                serviceSettingsMap,
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

    private static FireworksAiModel createModel(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        ChunkingSettings chunkingSettings,
        @Nullable Map<String, Object> secretSettings,
        ConfigurationParseContext context
    ) {
        return switch (taskType) {
            case TEXT_EMBEDDING -> new FireworksAiEmbeddingsModel(
                inferenceEntityId,
                taskType,
                NAME,
                serviceSettings,
                taskSettings,
                chunkingSettings,
                secretSettings,
                context
            );
            case RERANK -> new FireworksAiRerankModel(inferenceEntityId, serviceSettings, taskSettings, secretSettings, context);
            default -> throw createInvalidTaskTypeException(inferenceEntityId, NAME, taskType, context);
        };
    }

    @Override
    public FireworksAiModel parsePersistedConfigWithSecrets(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> config,
        Map<String, Object> secrets
    ) {
        Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
        Map<String, Object> taskSettingsMap = removeFromMapOrDefaultEmpty(config, ModelConfigurations.TASK_SETTINGS);
        Map<String, Object> secretSettingsMap = removeFromMapOrDefaultEmpty(secrets, ModelSecrets.SECRET_SETTINGS);

        ChunkingSettings chunkingSettings = null;
        if (TaskType.TEXT_EMBEDDING.equals(taskType)) {
            chunkingSettings = ChunkingSettingsBuilder.fromMap(removeFromMap(config, ModelConfigurations.CHUNKING_SETTINGS));
        }

        return createModel(
            inferenceEntityId,
            taskType,
            serviceSettingsMap,
            taskSettingsMap,
            chunkingSettings,
            secretSettingsMap,
            ConfigurationParseContext.PERSISTENT
        );
    }

    @Override
    public FireworksAiModel parsePersistedConfig(String inferenceEntityId, TaskType taskType, Map<String, Object> config) {
        Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
        Map<String, Object> taskSettingsMap = removeFromMapOrDefaultEmpty(config, ModelConfigurations.TASK_SETTINGS);

        ChunkingSettings chunkingSettings = null;
        if (TaskType.TEXT_EMBEDDING.equals(taskType)) {
            chunkingSettings = ChunkingSettingsBuilder.fromMap(removeFromMap(config, ModelConfigurations.CHUNKING_SETTINGS));
        }

        return createModel(
            inferenceEntityId,
            taskType,
            serviceSettingsMap,
            taskSettingsMap,
            chunkingSettings,
            null,
            ConfigurationParseContext.PERSISTENT
        );
    }

    @Override
    public void doInfer(
        Model model,
        InferenceInputs inputs,
        Map<String, Object> taskSettings,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        if (model instanceof FireworksAiModel == false) {
            listener.onFailure(createInvalidModelException(model));
            return;
        }

        FireworksAiModel fireworksAiModel = (FireworksAiModel) model;
        var actionCreator = new FireworksAiActionCreator(getSender(), getServiceComponents());

        var action = fireworksAiModel.accept(actionCreator, taskSettings);
        action.execute(inputs, timeout, listener);
    }

    @Override
    protected void doChunkedInfer(
        Model model,
        List<ChunkInferenceInput> inputs,
        Map<String, Object> taskSettings,
        InputType inputType,
        TimeValue timeout,
        ActionListener<List<ChunkedInference>> listener
    ) {
        if (model.getTaskType() == TaskType.RERANK) {
            listener.onFailure(
                new ElasticsearchStatusException("Chunked inference is not supported for rerank task", RestStatus.BAD_REQUEST)
            );
            return;
        }

        if (model instanceof FireworksAiEmbeddingsModel == false) {
            listener.onFailure(createInvalidModelException(model));
            return;
        }

        FireworksAiEmbeddingsModel embeddingsModel = (FireworksAiEmbeddingsModel) model;
        var actionCreator = new FireworksAiActionCreator(getSender(), getServiceComponents());

        List<EmbeddingRequestChunker.BatchRequestAndListener> batchedRequests = new EmbeddingRequestChunker<>(
            inputs,
            EMBEDDING_MAX_BATCH_SIZE,
            embeddingsModel.getConfigurations().getChunkingSettings()
        ).batchRequestsWithListeners(listener);

        for (var request : batchedRequests) {
            var action = embeddingsModel.accept(actionCreator, taskSettings);
            action.execute(new EmbeddingsInput(request.batch().inputs(), inputType), timeout, request.listener());
        }
    }

    @Override
    protected void doUnifiedCompletionInfer(
        Model model,
        UnifiedChatInput inputs,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        listener.onFailure(
            new ElasticsearchStatusException("Unified completion is not supported for FireworksAI service", RestStatus.BAD_REQUEST)
        );
    }

    @Override
    protected void validateInputType(InputType inputType, Model model, ValidationException validationException) {
        if (model.getTaskType() == TaskType.RERANK) {
            // Rerank accepts any input type
            return;
        }

        // For embeddings, validate input type is unspecified or internal
        ServiceUtils.validateInputTypeIsUnspecifiedOrInternal(inputType, validationException);
    }

    @Override
    public int rerankerWindowSize(String modelId) {
        // FireworksAI rerank models have a context window (adjust based on actual model limits)
        // Using a conservative estimate similar to ContextualAI
        return 5500;
    }

    @Override
    public InferenceServiceConfiguration getConfiguration() {
        return Configuration.get();
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return FIREWORKS_AI_SERVICE;
    }

    public static class Configuration {
        public static InferenceServiceConfiguration get() {
            return configuration.getOrCompute();
        }

        private static final LazyInitializable<InferenceServiceConfiguration, RuntimeException> configuration = new LazyInitializable<>(
            () -> {
                var configurationMap = new HashMap<String, SettingsConfiguration>();

                configurationMap.put(
                    MODEL_ID,
                    new SettingsConfiguration.Builder(SUPPORTED_TASK_TYPES).setDescription(
                        "The model ID to use for FireworksAI requests. Supports Qwen3 embeddings, Nomic embeddings, and reranker models."
                    )
                        .setLabel("Model ID")
                        .setRequired(true)
                        .setSensitive(false)
                        .setUpdatable(false)
                        .setType(SettingsConfigurationFieldType.STRING)
                        .build()
                );

                configurationMap.putAll(DefaultSecretSettings.toSettingsConfiguration(SUPPORTED_TASK_TYPES));
                configurationMap.putAll(RateLimitSettings.toSettingsConfiguration(SUPPORTED_TASK_TYPES));

                return new InferenceServiceConfiguration.Builder().setService(NAME)
                    .setName(SERVICE_NAME)
                    .setTaskTypes(SUPPORTED_TASK_TYPES)
                    .setConfigurations(configurationMap)
                    .build();
            }
        );
    }
}
