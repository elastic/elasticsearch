/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai;

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
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.voyageai.action.VoyageAIActionCreator;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.BaseVoyageAIEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.VoyageAIEmbeddingType;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.VoyageAIEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.VoyageAIEmbeddingsModelCreator;
import org.elasticsearch.xpack.inference.services.voyageai.rerank.VoyageAIRerankModelCreator;

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
import static org.elasticsearch.xpack.inference.services.voyageai.embeddings.BaseVoyageAIEmbeddingsServiceSettings.updateEmbeddingDetails;

public class VoyageAIService extends SenderService implements RerankingInferenceService {
    public static final String NAME = "voyageai";

    private static final String SERVICE_NAME = "Voyage AI";
    private static final EnumSet<TaskType> SUPPORTED_TASK_TYPES = EnumSet.of(
        TaskType.TEXT_EMBEDDING,
        TaskType.RERANK,
        TaskType.EMBEDDING
    );
    private static final VoyageAIEmbeddingsModelCreator EMBEDDINGS_MODEL_CREATOR = new VoyageAIEmbeddingsModelCreator();
    private static final Map<TaskType, ModelCreator<? extends VoyageAIModel>> MODEL_CREATORS = Map.of(
        TaskType.TEXT_EMBEDDING,
        EMBEDDINGS_MODEL_CREATOR,
        TaskType.EMBEDDING,
        EMBEDDINGS_MODEL_CREATOR,
        TaskType.RERANK,
        new VoyageAIRerankModelCreator()
    );
    private static final Integer DEFAULT_BATCH_SIZE = 7;
    private static final Map<String, Integer> MODEL_BATCH_SIZES = Map.ofEntries(
        Map.entry("voyage-multimodal-3", 7),
        Map.entry("voyage-multimodal-3.5", 7),
        Map.entry("voyage-3-large", 7),
        Map.entry("voyage-code-3", 7),
        Map.entry("voyage-3", 10),
        Map.entry("voyage-3.5", 10),
        Map.entry("voyage-3-lite", 30),
        Map.entry("voyage-3.5-lite", 30),
        Map.entry("voyage-finance-2", 7),
        Map.entry("voyage-law-2", 7),
        Map.entry("voyage-code-2", 7),
        Map.entry("voyage-2", 72),
        Map.entry("voyage-02", 72),
        Map.entry("voyage-4-large", 7),
        Map.entry("voyage-4", 10),
        Map.entry("voyage-4-lite", 30)
    );

    private static final Map<String, Integer> RERANKERS_INPUT_SIZE = Map.of(
        "rerank-lite-1",
        2800 // The smallest model has a 4K context length https://docs.voyageai.com/docs/reranker
    );

    /**
     * Apart from rerank-lite-1 all other models have a context length of at least 8k.
     * This value is based on 1 token == 0.75 words and allowing for some overhead
     */
    private static final int DEFAULT_RERANKER_INPUT_SIZE_WORDS = 5500;

    public static final EnumSet<InputType> VALID_INPUT_TYPE_VALUES = EnumSet.of(
        InputType.INGEST,
        InputType.SEARCH,
        InputType.INTERNAL_INGEST,
        InputType.INTERNAL_SEARCH
    );

    private static final TransportVersion VOYAGE_AI_INTEGRATION_ADDED = TransportVersion.fromName("voyage_ai_integration_added");

    public VoyageAIService(
        HttpRequestSender.Factory factory,
        ServiceComponents serviceComponents,
        InferenceServiceExtension.InferenceServiceFactoryContext context
    ) {
        this(factory, serviceComponents, context.clusterService());
    }

    public VoyageAIService(HttpRequestSender.Factory factory, ServiceComponents serviceComponents, ClusterService clusterService) {
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
            VoyageAIModel model = createModel(
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

    private static VoyageAIModel createModelFromPersistent(
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

    private static VoyageAIModel createModel(
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
    public VoyageAIModel parsePersistedConfigWithSecrets(
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
    public VoyageAIModel parsePersistedConfig(String inferenceEntityId, TaskType taskType, Map<String, Object> config) {
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
        if (model instanceof VoyageAIModel == false) {
            listener.onFailure(createInvalidModelException(model));
            return;
        }

        VoyageAIModel voyageaiModel = (VoyageAIModel) model;
        var actionCreator = new VoyageAIActionCreator(getSender(), getServiceComponents());

        var action = voyageaiModel.accept(actionCreator, taskSettings);
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
        if (model instanceof VoyageAIModel == false) {
            listener.onFailure(createInvalidModelException(model));
            return;
        }

        VoyageAIModel voyageaiModel = (VoyageAIModel) model;
        var actionCreator = new VoyageAIActionCreator(getSender(), getServiceComponents());

        List<EmbeddingRequestChunker.BatchRequestAndListener> batchedRequests = new EmbeddingRequestChunker<>(
            inputs,
            getBatchSize(voyageaiModel),
            voyageaiModel.getConfigurations().getChunkingSettings()
        ).batchRequestsWithListeners(listener);

        for (var request : batchedRequests) {
            var action = voyageaiModel.accept(actionCreator, taskSettings);
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
        if (model instanceof VoyageAIEmbeddingsModel voyageAIModel) {
            if (model.getServiceSettings().isMultimodal() == false && containsNonTextEntry(request.inputs())) {
                listener.onFailure(new ElasticsearchStatusException("Non-text input provided for text-only model", RestStatus.BAD_REQUEST));
            } else {
                var actionCreator = new VoyageAIActionCreator(getSender(), getServiceComponents());

                ExecutableAction action = voyageAIModel.accept(actionCreator, request.taskSettings());
                action.execute(new EmbeddingsInput(request::inputs, request.inputType()), timeout, listener);
            }
        } else {
            listener.onFailure(createInvalidModelException(model));
        }
    }

    private static int getBatchSize(VoyageAIModel model) {
        return MODEL_BATCH_SIZES.getOrDefault(model.getServiceSettings().modelId(), DEFAULT_BATCH_SIZE);
    }

    @Override
    public Model updateModelWithEmbeddingDetails(Model model, int embeddingSize) {
        if (model instanceof VoyageAIEmbeddingsModel embeddingsModel) {
            var serviceSettings = embeddingsModel.getServiceSettings();
            var similarityFromModel = serviceSettings.similarity();
            var similarityToUse = similarityFromModel == null ? defaultSimilarity() : similarityFromModel;

            var updatedServiceSettings = updateEmbeddingDetails(serviceSettings, embeddingSize, similarityToUse);

            if (updatedServiceSettings.equals(serviceSettings)) {
                return model;
            }

            return new VoyageAIEmbeddingsModel(embeddingsModel, updatedServiceSettings);
        } else {
            throw ServiceUtils.invalidModelTypeForUpdateModelWithEmbeddingDetails(model.getClass());
        }
    }

    /**
     * Return the default similarity measure for the embedding type.
     * VoyageAI embeddings are normalized to unit vectors therefore Dot
     * Product similarity can be used and is the default for all VoyageAI
     * models.
     *
     * @return The default similarity.
     */
    static SimilarityMeasure defaultSimilarity() {
        return SimilarityMeasure.DOT_PRODUCT;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return VOYAGE_AI_INTEGRATION_ADDED;
    }

    @Override
    public int rerankerWindowSize(String modelId) {
        Integer inputSize = RERANKERS_INPUT_SIZE.get(modelId);
        return inputSize != null ? inputSize : DEFAULT_RERANKER_INPUT_SIZE_WORDS;
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
                        "The number of dimensions the resulting embeddings should have."
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
                            "The type of embedding to return. One of %s. int8 and byte are equivalent and are encoded as "
                                + "bytes with signed int8 precision. bit and binary are equivalent.",
                            EnumSet.allOf(VoyageAIEmbeddingType.class)
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
                            "The similarity measure. One of %s. The default similarity is dot_product.",
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
