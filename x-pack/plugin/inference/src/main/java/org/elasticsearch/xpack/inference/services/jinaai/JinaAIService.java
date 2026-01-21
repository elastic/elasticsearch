/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkInferenceInput;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.EmbeddingRequest;
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
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsBuilder;
import org.elasticsearch.xpack.core.inference.chunking.EmbeddingRequestChunker;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ModelCreator;
import org.elasticsearch.xpack.inference.services.SenderService;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.jinaai.action.JinaAIActionCreator;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingType;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsModelCreator;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsTaskSettings;
import org.elasticsearch.xpack.inference.services.jinaai.rerank.JinaAIRerankModelCreator;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.inference.InferenceStringGroup.containsNonTextEntry;
import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.EMBEDDING_TYPE;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createInvalidModelException;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMap;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrDefaultEmpty;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrThrowIfNull;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.throwIfNotEmptyMap;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.throwUnsupportedUnifiedCompletionOperation;
import static org.elasticsearch.xpack.inference.services.jinaai.JinaAIServiceFields.EMBEDDING_MAX_BATCH_SIZE;
import static org.elasticsearch.xpack.inference.services.jinaai.embeddings.BaseJinaAIEmbeddingsServiceSettings.updateEmbeddingDetails;

public class JinaAIService extends SenderService implements RerankingInferenceService {

    public static final TransportVersion JINA_AI_EMBEDDING_REFACTOR = TransportVersion.fromName("jina_ai_embedding_refactor");

    public static final String NAME = "jinaai";

    private static final String SERVICE_NAME = "Jina AI";
    private static final EnumSet<TaskType> SUPPORTED_TASK_TYPES = EnumSet.of(TaskType.TEXT_EMBEDDING, TaskType.RERANK, TaskType.EMBEDDING);
    private static final JinaAIEmbeddingsModelCreator EMBEDDINGS_MODEL_CREATOR = new JinaAIEmbeddingsModelCreator();
    private static final Map<TaskType, ModelCreator<? extends JinaAIModel>> MODEL_CREATORS = Map.of(
        TaskType.TEXT_EMBEDDING,
        EMBEDDINGS_MODEL_CREATOR,
        TaskType.EMBEDDING,
        EMBEDDINGS_MODEL_CREATOR,
        TaskType.RERANK,
        new JinaAIRerankModelCreator()
    );
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
            if (TaskType.TEXT_EMBEDDING.equals(taskType) || TaskType.EMBEDDING.equals(taskType)) {
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
        return retrieveModelCreatorFromMapOrThrow(MODEL_CREATORS, inferenceEntityId, taskType, NAME, context).createFromMaps(
            inferenceEntityId,
            taskType,
            NAME,
            serviceSettings,
            taskSettings,
            chunkingSettings,
            secretSettings,
            context
        );
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
        if (TaskType.TEXT_EMBEDDING.equals(taskType) || TaskType.EMBEDDING.equals(taskType)) {
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
    public Model buildModelFromConfigAndSecrets(ModelConfigurations config, ModelSecrets secrets) {
        return retrieveModelCreatorFromMapOrThrow(
            MODEL_CREATORS,
            config.getInferenceEntityId(),
            config.getTaskType(),
            config.getService(),
            ConfigurationParseContext.PERSISTENT
        ).createFromModelConfigurationsAndSecrets(config, secrets);
    }

    @Override
    public JinaAIModel parsePersistedConfig(String inferenceEntityId, TaskType taskType, Map<String, Object> config) {
        Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
        Map<String, Object> taskSettingsMap = removeFromMapOrDefaultEmpty(config, ModelConfigurations.TASK_SETTINGS);

        ChunkingSettings chunkingSettings = null;
        if (TaskType.TEXT_EMBEDDING.equals(taskType) || TaskType.EMBEDDING.equals(taskType)) {
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
        return SUPPORTED_TASK_TYPES;
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
                || jinaAIEmbeddingsTaskSettings.getLateChunking() == false
                || inputs.stream().anyMatch(c -> c.input().containsNonTextEntry());
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
    protected void doEmbeddingInfer(
        Model model,
        EmbeddingRequest request,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        if (model instanceof JinaAIEmbeddingsModel jinaAIModel) {
            if (model.getServiceSettings().isMultimodal() == false && containsNonTextEntry(request.inputs())) {
                listener.onFailure(new ElasticsearchStatusException("Non-text input provided for text-only model", RestStatus.BAD_REQUEST));
            } else {
                var actionCreator = new JinaAIActionCreator(getSender(), getServiceComponents());

                ExecutableAction action = jinaAIModel.accept(actionCreator, request.taskSettings());
                action.execute(new EmbeddingsInput(request::inputs, request.inputType()), timeout, listener);
            }
        } else {
            listener.onFailure(createInvalidModelException(model));
        }

    }

    @Override
    public Model updateModelWithEmbeddingDetails(Model model, int embeddingSize) {
        if (model instanceof JinaAIEmbeddingsModel embeddingsModel) {
            var serviceSettings = embeddingsModel.getServiceSettings();
            var similarityFromModel = serviceSettings.similarity();
            var similarityToUse = similarityFromModel == null ? defaultSimilarity(serviceSettings.getEmbeddingType()) : similarityFromModel;

            var updatedServiceSettings = updateEmbeddingDetails(serviceSettings, embeddingSize, similarityToUse);

            if (updatedServiceSettings.equals(serviceSettings)) {
                return model;
            }

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
        return TransportVersion.minimumCompatible();
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
            return CONFIGURATION.getOrCompute();
        }

        private static final LazyInitializable<InferenceServiceConfiguration, RuntimeException> CONFIGURATION = new LazyInitializable<>(
            () -> {
                var configurationMap = new HashMap<String, SettingsConfiguration>();

                configurationMap.put(
                    MODEL_ID,
                    new SettingsConfiguration.Builder(SUPPORTED_TASK_TYPES).setDescription(
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
                    new SettingsConfiguration.Builder(EnumSet.of(TaskType.TEXT_EMBEDDING, TaskType.EMBEDDING)).setDescription(
                        "The number of dimensions the resulting embeddings should have. For more information refer to "
                            + "https://api.jina.ai/docs#tag/embeddings/operation/create_embedding_v1_embeddings_post."
                    )
                        .setLabel("Dimensions")
                        .setRequired(false)
                        .setSensitive(false)
                        .setUpdatable(false)
                        .setType(SettingsConfigurationFieldType.INTEGER)
                        .build()
                );

                configurationMap.put(
                    EMBEDDING_TYPE,
                    new SettingsConfiguration.Builder(EnumSet.of(TaskType.TEXT_EMBEDDING, TaskType.EMBEDDING)).setDescription(
                        Strings.format(
                            "The type of embedding to return. One of %s. bit and binary are equivalent and are encoded as "
                                + "bytes with signed int8 precision.",
                            EnumSet.allOf(JinaAIEmbeddingType.class)
                        )
                    )
                        .setLabel("Embedding type")
                        .setDefaultValue("float")
                        .setRequired(false)
                        .setSensitive(false)
                        .setUpdatable(false)
                        .setType(SettingsConfigurationFieldType.STRING)
                        .build()
                );

                configurationMap.put(
                    SIMILARITY,
                    new SettingsConfiguration.Builder(EnumSet.of(TaskType.TEXT_EMBEDDING, TaskType.EMBEDDING)).setDescription(
                        Strings.format(
                            "The similarity measure. One of %s. For float embeddings, the default similarity "
                                + "is dot_product. For bit and binary embeddings, the default similarity is l2_norm.",
                            EnumSet.allOf(SimilarityMeasure.class)
                        )
                    )
                        .setLabel("Similarity")
                        .setRequired(false)
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
