/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.nvidia;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.util.LazyInitializable;
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
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.external.http.sender.GenericRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.SenderService;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.nvidia.action.NvidiaActionCreator;
import org.elasticsearch.xpack.inference.services.nvidia.completion.NvidiaChatCompletionModel;
import org.elasticsearch.xpack.inference.services.nvidia.completion.NvidiaChatCompletionResponseHandler;
import org.elasticsearch.xpack.inference.services.nvidia.embeddings.NvidiaEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.nvidia.embeddings.NvidiaEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.nvidia.request.completion.NvidiaChatCompletionRequest;
import org.elasticsearch.xpack.inference.services.nvidia.rerank.NvidiaRerankModel;
import org.elasticsearch.xpack.inference.services.openai.response.OpenAiChatCompletionResponseEntity;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.inference.TaskType.CHAT_COMPLETION;
import static org.elasticsearch.inference.TaskType.COMPLETION;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createInvalidModelException;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createInvalidTaskTypeException;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMap;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrDefaultEmpty;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrThrowIfNull;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.throwIfNotEmptyMap;

/**
 * NvidiaService is an inference service for Nvidia models, supporting text embedding and chat completion tasks.
 * It extends {@link SenderService} to handle HTTP requests and responses for Nvidia models.
 */
public class NvidiaService extends SenderService implements RerankingInferenceService {
    public static final String NAME = "nvidia";
    private static final String SERVICE_NAME = "Nvidia";

    public static final EnumSet<InputType> VALID_INPUT_TYPE_VALUES = EnumSet.of(
        InputType.INGEST,
        InputType.SEARCH,
        InputType.INTERNAL_INGEST,
        InputType.INTERNAL_SEARCH
    );
    /**
     * The optimal batch size depends on the model.
     * For Nvidia use a conservatively small max batch size as it is unknown what model is used.
     */
    static final int EMBEDDING_MAX_BATCH_SIZE = 20;
    private static final EnumSet<TaskType> SUPPORTED_TASK_TYPES = EnumSet.of(
        TaskType.TEXT_EMBEDDING,
        TaskType.COMPLETION,
        TaskType.CHAT_COMPLETION,
        TaskType.RERANK
    );
    private static final ResponseHandler UNIFIED_CHAT_COMPLETION_HANDLER = new NvidiaChatCompletionResponseHandler(
        "Nvidia chat completion",
        OpenAiChatCompletionResponseEntity::fromResponse
    );

    /**
     * Constructor for creating an {@link NvidiaService} with specified HTTP request sender factory and service components.
     *
     * @param factory the factory to create HTTP request senders
     * @param serviceComponents the components required for the inference service
     * @param context the context for the inference service factory
     */
    public NvidiaService(
        HttpRequestSender.Factory factory,
        ServiceComponents serviceComponents,
        InferenceServiceExtension.InferenceServiceFactoryContext context
    ) {
        this(factory, serviceComponents, context.clusterService());
    }

    public NvidiaService(HttpRequestSender.Factory factory, ServiceComponents serviceComponents, ClusterService clusterService) {
        super(factory, serviceComponents, clusterService);
    }

    @Override
    protected void doInfer(
        Model model,
        InferenceInputs inputs,
        Map<String, Object> taskSettings,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        var actionCreator = new NvidiaActionCreator(getSender(), getServiceComponents());
        if (model instanceof NvidiaModel nvidiaModel) {
            nvidiaModel.accept(actionCreator, taskSettings).execute(inputs, timeout, listener);
        } else {
            listener.onFailure(createInvalidModelException(model));
        }
    }

    /**
     * Creates an {@link NvidiaModel} based on the provided parameters.
     *
     * @param inferenceId the unique identifier for the inference entity
     * @param taskType the type of task this model is designed for
     * @param serviceSettings the settings for the inference service
     * @param taskSettings the task-specific settings, if applicable
     * @param chunkingSettings the settings for chunking, if applicable
     * @param secretSettings the secret settings for the model, such as API keys or tokens
     * @param context the context for parsing configuration settings
     * @return a new instance of {@link NvidiaModel} based on the provided parameters
     */
    protected NvidiaModel createModel(
        String inferenceId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        ChunkingSettings chunkingSettings,
        Map<String, Object> secretSettings,
        ConfigurationParseContext context
    ) {
        return switch (taskType) {
            case CHAT_COMPLETION, COMPLETION -> new NvidiaChatCompletionModel(
                inferenceId,
                taskType,
                NAME,
                serviceSettings,
                secretSettings,
                context
            );
            case TEXT_EMBEDDING -> new NvidiaEmbeddingsModel(
                inferenceId,
                taskType,
                NAME,
                serviceSettings,
                taskSettings,
                chunkingSettings,
                secretSettings,
                context
            );
            case RERANK -> new NvidiaRerankModel(inferenceId, taskType, NAME, serviceSettings, secretSettings, context);
            default -> throw createInvalidTaskTypeException(inferenceId, NAME, taskType, context);
        };
    }

    @Override
    protected void doUnifiedCompletionInfer(
        Model model,
        UnifiedChatInput inputs,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        if (model instanceof NvidiaChatCompletionModel == false) {
            listener.onFailure(createInvalidModelException(model));
            return;
        }

        var nvidiaChatCompletionModel = (NvidiaChatCompletionModel) model;
        var overriddenModel = NvidiaChatCompletionModel.of(nvidiaChatCompletionModel, inputs.getRequest().model());
        var manager = new GenericRequestManager<>(
            getServiceComponents().threadPool(),
            overriddenModel,
            UNIFIED_CHAT_COMPLETION_HANDLER,
            unifiedChatInput -> new NvidiaChatCompletionRequest(unifiedChatInput, overriddenModel),
            UnifiedChatInput.class
        );
        var errorMessage = NvidiaActionCreator.buildErrorMessage(CHAT_COMPLETION, model.getInferenceEntityId());
        var action = new SenderExecutableAction(getSender(), manager, errorMessage);

        action.execute(inputs, timeout, listener);
    }

    @Override
    public Model updateModelWithEmbeddingDetails(Model model, int embeddingSize) {
        if (model instanceof NvidiaEmbeddingsModel embeddingsModel) {
            var serviceSettings = embeddingsModel.getServiceSettings();
            var similarityFromModel = serviceSettings.similarity();
            var similarityToUse = similarityFromModel == null ? SimilarityMeasure.DOT_PRODUCT : similarityFromModel;

            if (similarityToUse.equals(similarityFromModel) && embeddingSize == serviceSettings.dimensions()) {
                // Avoid creating a new model if similarity and embedding size are unchanged
                return model;
            }
            var updatedServiceSettings = new NvidiaEmbeddingsServiceSettings(
                serviceSettings.modelId(),
                serviceSettings.uri(),
                embeddingSize,
                similarityToUse,
                serviceSettings.maxInputTokens(),
                serviceSettings.rateLimitSettings()
            );

            return new NvidiaEmbeddingsModel(embeddingsModel, updatedServiceSettings);
        } else {
            throw ServiceUtils.invalidModelTypeForUpdateModelWithEmbeddingDetails(model.getClass());
        }
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
        if (model instanceof NvidiaEmbeddingsModel == false) {
            listener.onFailure(createInvalidModelException(model));
            return;
        }

        var nvidiaEmbeddingsModel = (NvidiaEmbeddingsModel) model;
        var actionCreator = new NvidiaActionCreator(getSender(), getServiceComponents());

        List<EmbeddingRequestChunker.BatchRequestAndListener> batchedRequests = new EmbeddingRequestChunker<>(
            inputs,
            EMBEDDING_MAX_BATCH_SIZE,
            nvidiaEmbeddingsModel.getConfigurations().getChunkingSettings()
        ).batchRequestsWithListeners(listener);

        for (var request : batchedRequests) {
            var action = nvidiaEmbeddingsModel.accept(actionCreator, taskSettings);
            action.execute(new EmbeddingsInput(request.batch().inputs(), inputType), timeout, request.listener());
        }
    }

    @Override
    public Set<TaskType> supportedStreamingTasks() {
        return EnumSet.of(COMPLETION, CHAT_COMPLETION);
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
    public String name() {
        return NAME;
    }

    @Override
    public void parseRequestConfig(
        String inferenceId,
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

            NvidiaModel model = createModel(
                inferenceId,
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

    private NvidiaModel createModelFromPersistent(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        ChunkingSettings chunkingSettings,
        Map<String, Object> secretSettings
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

    private NvidiaModel parsePersistedConfigInternal(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> config,
        Map<String, Object> secrets
    ) {
        Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
        Map<String, Object> taskSettingsMap = removeFromMapOrDefaultEmpty(config, ModelConfigurations.TASK_SETTINGS);
        Map<String, Object> secretSettingsMap = null;
        if (secrets != null) {
            secretSettingsMap = removeFromMapOrDefaultEmpty(secrets, ModelSecrets.SECRET_SETTINGS);
        }

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
    public NvidiaModel parsePersistedConfigWithSecrets(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> config,
        Map<String, Object> secrets
    ) {
        return parsePersistedConfigInternal(inferenceEntityId, taskType, config, secrets);
    }

    @Override
    public NvidiaModel parsePersistedConfig(String inferenceEntityId, TaskType taskType, Map<String, Object> config) {
        return parsePersistedConfigInternal(inferenceEntityId, taskType, config, null);
    }

    @Override
    protected void validateInputType(InputType inputType, Model model, ValidationException validationException) {
        ServiceUtils.validateInputTypeAgainstAllowlist(inputType, VALID_INPUT_TYPE_VALUES, SERVICE_NAME, validationException);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return NvidiaUtils.ML_INFERENCE_NVIDIA_ADDED;
    }

    @Override
    public int rerankerWindowSize(String modelId) {
        // Nvidia reranking models have a max window size of 512 input tokens, so use a conservative default of 300 words
        // Documentation: https://docs.api.nvidia.com/nim/reference/nvidia-llama-3_2-nv-rerankqa-1b-v1
        return CONSERVATIVE_DEFAULT_WINDOW_SIZE;
    }

    /**
     * Configuration class for the Nvidia inference service.
     * It provides the settings and configurations required for the service.
     */
    public static class Configuration {
        public static InferenceServiceConfiguration get() {
            return CONFIGURATION.getOrCompute();
        }

        private Configuration() {}

        private static final LazyInitializable<InferenceServiceConfiguration, RuntimeException> CONFIGURATION = new LazyInitializable<>(
            () -> {
                var configurationMap = new HashMap<String, SettingsConfiguration>();

                configurationMap.put(
                    URL,
                    new SettingsConfiguration.Builder(SUPPORTED_TASK_TYPES).setDescription("The URL endpoint to use for the requests.")
                        .setLabel("URL")
                        .setRequired(false)
                        .setSensitive(false)
                        .setUpdatable(false)
                        .setType(SettingsConfigurationFieldType.STRING)
                        .build()
                );
                configurationMap.put(
                    MODEL_ID,
                    new SettingsConfiguration.Builder(SUPPORTED_TASK_TYPES).setDescription("""
                        The name of the model to use for the inference task. Refer to the \
                        Nvidia models documentation for the list of available models.""")
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
