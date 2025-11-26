/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
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
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.configuration.SettingsConfigurationFieldType;
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
import org.elasticsearch.xpack.inference.services.jinaai.action.JinaAIActionCreator;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingType;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsTaskSettings;
import org.elasticsearch.xpack.inference.services.jinaai.rerank.JinaAIRerankModel;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createInvalidModelException;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createInvalidTaskTypeException;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMap;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrDefaultEmpty;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrThrowIfNull;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.throwIfNotEmptyMap;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.throwUnsupportedUnifiedCompletionOperation;
import static org.elasticsearch.xpack.inference.services.jinaai.JinaAIServiceFields.EMBEDDING_MAX_BATCH_SIZE;

public class JinaAIService extends SenderService implements RerankingInferenceService {
    public static final String NAME = "jinaai";

    private static final String SERVICE_NAME = "Jina AI";
    private static final EnumSet<TaskType> supportedTaskTypes = EnumSet.of(TaskType.TEXT_EMBEDDING, TaskType.RERANK);

    public static final EnumSet<InputType> VALID_INPUT_TYPE_VALUES = EnumSet.of(
        InputType.INGEST,
        InputType.SEARCH,
        InputType.CLASSIFICATION,
        InputType.CLUSTERING,
        InputType.INTERNAL_INGEST,
        InputType.INTERNAL_SEARCH
    );

    public JinaAIService(
        HttpRequestSender.Factory factory,
        ServiceComponents serviceComponents,
        InferenceServiceExtension.InferenceServiceFactoryContext context
    ) {
        this(factory, serviceComponents, context.clusterService());
    }

    public JinaAIService(HttpRequestSender.Factory factory, ServiceComponents serviceComponents, ClusterService clusterService) {
        super(factory, serviceComponents, clusterService);
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
            JinaAIModel model = createModel(
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

    private static JinaAIModel createModelFromPersistent(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        ChunkingSettings chunkingSettings,
        @Nullable Map<String, Object> secretSettings
    ) {
        return createModel(
            inferenceEntityId,
            taskType,
            serviceSettings,
            taskSettings,
            chunkingSettings,
            secretSettings,
            ConfigurationParseContext.PERSISTENT
        );
    }

    private static JinaAIModel createModel(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        ChunkingSettings chunkingSettings,
        @Nullable Map<String, Object> secretSettings,
        ConfigurationParseContext context
    ) {
        return switch (taskType) {
            case TEXT_EMBEDDING -> new JinaAIEmbeddingsModel(
                inferenceEntityId,
                NAME,
                serviceSettings,
                taskSettings,
                chunkingSettings,
                secretSettings,
                context
            );
            case RERANK -> new JinaAIRerankModel(inferenceEntityId, NAME, serviceSettings, taskSettings, secretSettings, context);
            default -> throw createInvalidTaskTypeException(inferenceEntityId, NAME, taskType, context);
        };
    }

    @Override
    public JinaAIModel parsePersistedConfigWithSecrets(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> config,
        Map<String, Object> secrets
    ) {
        Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
        Map<String, Object> taskSettingsMap = removeFromMapOrDefaultEmpty(config, ModelConfigurations.TASK_SETTINGS);
        Map<String, Object> secretSettingsMap = removeFromMapOrThrowIfNull(secrets, ModelSecrets.SECRET_SETTINGS);

        ChunkingSettings chunkingSettings = null;
        if (TaskType.TEXT_EMBEDDING.equals(taskType)) {
            chunkingSettings = ChunkingSettingsBuilder.fromMap(removeFromMap(config, ModelConfigurations.CHUNKING_SETTINGS));
        }

        return createModelFromPersistent(
            inferenceEntityId,
            taskType,
            serviceSettingsMap,
            taskSettingsMap,
            chunkingSettings,
            secretSettingsMap
        );
    }

    @Override
    public JinaAIModel parsePersistedConfig(String inferenceEntityId, TaskType taskType, Map<String, Object> config) {
        Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
        Map<String, Object> taskSettingsMap = removeFromMapOrDefaultEmpty(config, ModelConfigurations.TASK_SETTINGS);

        ChunkingSettings chunkingSettings = null;
        if (TaskType.TEXT_EMBEDDING.equals(taskType)) {
            chunkingSettings = ChunkingSettingsBuilder.fromMap(removeFromMap(config, ModelConfigurations.CHUNKING_SETTINGS));
        }

        return createModelFromPersistent(inferenceEntityId, taskType, serviceSettingsMap, taskSettingsMap, chunkingSettings, null);
    }

    @Override
    public InferenceServiceConfiguration getConfiguration() {
        return Configuration.get();
    }

    @Override
    public EnumSet<TaskType> supportedTaskTypes() {
        return supportedTaskTypes;
    }

    @Override
    protected void doUnifiedCompletionInfer(
        Model model,
        UnifiedChatInput inputs,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        throwUnsupportedUnifiedCompletionOperation(NAME);
    }

    @Override
    public void doInfer(
        Model model,
        InferenceInputs inputs,
        Map<String, Object> taskSettings,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        if (model instanceof JinaAIModel == false) {
            listener.onFailure(createInvalidModelException(model));
            return;
        }

        JinaAIModel jinaaiModel = (JinaAIModel) model;
        var actionCreator = new JinaAIActionCreator(getSender(), getServiceComponents());

        var action = jinaaiModel.accept(actionCreator, taskSettings);
        action.execute(inputs, timeout, listener);
    }

    @Override
    protected void validateInputType(InputType inputType, Model model, ValidationException validationException) {
        ServiceUtils.validateInputTypeAgainstAllowlist(inputType, VALID_INPUT_TYPE_VALUES, SERVICE_NAME, validationException);
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
        if (model instanceof JinaAIModel == false) {
            listener.onFailure(createInvalidModelException(model));
            return;
        }

        JinaAIModel jinaaiModel = (JinaAIModel) model;
        var actionCreator = new JinaAIActionCreator(getSender(), getServiceComponents());

        boolean batchChunksAcrossInputs = true;
        if (jinaaiModel.getTaskSettings() instanceof JinaAIEmbeddingsTaskSettings jinaAIEmbeddingsTaskSettings) {
            batchChunksAcrossInputs = jinaAIEmbeddingsTaskSettings.getLateChunking() == null
                || jinaAIEmbeddingsTaskSettings.getLateChunking() == false;
        }

        List<EmbeddingRequestChunker.BatchRequestAndListener> batchedRequests = new EmbeddingRequestChunker<>(
            inputs,
            EMBEDDING_MAX_BATCH_SIZE,
            batchChunksAcrossInputs,
            jinaaiModel.getConfigurations().getChunkingSettings()
        ).batchRequestsWithListeners(listener);

        for (var request : batchedRequests) {
            var action = jinaaiModel.accept(actionCreator, taskSettings);
            action.execute(new EmbeddingsInput(request.batch().inputs(), inputType), timeout, request.listener());
        }
    }

    @Override
    public Model updateModelWithEmbeddingDetails(Model model, int embeddingSize) {
        if (model instanceof JinaAIEmbeddingsModel embeddingsModel) {
            var serviceSettings = embeddingsModel.getServiceSettings();
            var similarityFromModel = serviceSettings.similarity();
            var similarityToUse = similarityFromModel == null ? defaultSimilarity(serviceSettings.getEmbeddingType()) : similarityFromModel;
            var maxInputTokens = serviceSettings.maxInputTokens();

            var updatedServiceSettings = new JinaAIEmbeddingsServiceSettings(
                new JinaAIServiceSettings(
                    serviceSettings.getCommonSettings().uri(),
                    serviceSettings.getCommonSettings().modelId(),
                    serviceSettings.getCommonSettings().rateLimitSettings()
                ),
                similarityToUse,
                embeddingSize,
                maxInputTokens,
                serviceSettings.getEmbeddingType()
            );

            return new JinaAIEmbeddingsModel(embeddingsModel, updatedServiceSettings);
        } else {
            throw ServiceUtils.invalidModelTypeForUpdateModelWithEmbeddingDetails(model.getClass());
        }
    }

    /**
     * Return the default similarity measure for the embedding type.
     * JinaAI embeddings are normalized to unit vectors therefore Dot
     * Product similarity can be used and is the default for all JinaAI
     * models.
     *
     * @return The default similarity.
     */
    static SimilarityMeasure defaultSimilarity(JinaAIEmbeddingType embeddingType) {
        if (embeddingType == JinaAIEmbeddingType.BINARY || embeddingType == JinaAIEmbeddingType.BIT) {
            return SimilarityMeasure.L2_NORM;
        }
        return SimilarityMeasure.DOT_PRODUCT;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_18_0;
    }

    @Override
    public int rerankerWindowSize(String modelId) {
        // Jina AI rerank models have an 8000 token input length https://jina.ai/models/jina-reranker-v2-base-multilingual
        // Using 1 token = 0.75 words as a rough estimate, we get 6000 words
        // allowing for some headroom, we set the window size below 6000 words
        return 5500;
    }

    public static class Configuration {
        public static InferenceServiceConfiguration get() {
            return configuration.getOrCompute();
        }

        private static final LazyInitializable<InferenceServiceConfiguration, RuntimeException> configuration = new LazyInitializable<>(
            () -> {
                var configurationMap = new HashMap<String, SettingsConfiguration>();

                configurationMap.put(
                    JinaAIServiceSettings.MODEL_ID,
                    new SettingsConfiguration.Builder(supportedTaskTypes).setDescription(
                        "The name of the model to use for the inference task."
                    )
                        .setLabel("Model ID")
                        .setRequired(true)
                        .setSensitive(false)
                        .setUpdatable(false)
                        .setType(SettingsConfigurationFieldType.STRING)
                        .build()
                );

                configurationMap.put(
                    DIMENSIONS,
                    new SettingsConfiguration.Builder(EnumSet.of(TaskType.TEXT_EMBEDDING)).setDescription(
                        "The number of dimensions the resulting embeddings should have. For more information refer to "
                            + "https://api.jina.ai/redoc#tag/embeddings/operation/create_embedding_v1_embeddings_post."
                    )
                        .setLabel("Dimensions")
                        .setRequired(false)
                        .setSensitive(false)
                        .setUpdatable(false)
                        .setType(SettingsConfigurationFieldType.INTEGER)
                        .build()
                );

                configurationMap.putAll(DefaultSecretSettings.toSettingsConfiguration(supportedTaskTypes));
                configurationMap.putAll(RateLimitSettings.toSettingsConfiguration(supportedTaskTypes));

                return new InferenceServiceConfiguration.Builder().setService(NAME)
                    .setName(SERVICE_NAME)
                    .setTaskTypes(supportedTaskTypes)
                    .setConfigurations(configurationMap)
                    .build();
            }
        );
    }
}
