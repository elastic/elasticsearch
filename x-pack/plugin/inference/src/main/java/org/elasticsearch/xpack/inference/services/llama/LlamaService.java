/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.llama;

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
import org.elasticsearch.xpack.inference.services.llama.action.LlamaActionCreator;
import org.elasticsearch.xpack.inference.services.llama.completion.LlamaChatCompletionModel;
import org.elasticsearch.xpack.inference.services.llama.completion.LlamaChatCompletionResponseHandler;
import org.elasticsearch.xpack.inference.services.llama.embeddings.LlamaEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.llama.embeddings.LlamaEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.llama.request.completion.LlamaChatCompletionRequest;
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
import static org.elasticsearch.inference.TaskType.TEXT_EMBEDDING;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createInvalidModelException;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createInvalidTaskTypeException;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMap;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrDefaultEmpty;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrThrowIfNull;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.throwIfNotEmptyMap;

/**
 * LlamaService is an inference service for Llama models, supporting text embedding and chat completion tasks.
 * It extends SenderService to handle HTTP requests and responses for Llama models.
 */
public class LlamaService extends SenderService {
    public static final String NAME = "llama";
    private static final String SERVICE_NAME = "Llama";
    private static final TransportVersion ML_INFERENCE_LLAMA_ADDED = TransportVersion.fromName("ml_inference_llama_added");
    /**
     * The optimal batch size depends on the hardware the model is deployed on.
     * For Llama use a conservatively small max batch size as it is
     * unknown how the model is deployed
     */
    static final int EMBEDDING_MAX_BATCH_SIZE = 20;
    private static final EnumSet<TaskType> SUPPORTED_TASK_TYPES = EnumSet.of(TEXT_EMBEDDING, COMPLETION, CHAT_COMPLETION);
    private static final ResponseHandler UNIFIED_CHAT_COMPLETION_HANDLER = new LlamaChatCompletionResponseHandler(
        "llama chat completion",
        OpenAiChatCompletionResponseEntity::fromResponse
    );

    /**
     * Constructor for creating a LlamaService with specified HTTP request sender factory and service components.
     *
     * @param factory the factory to create HTTP request senders
     * @param serviceComponents the components required for the inference service
     * @param context the context for the inference service factory
     */
    public LlamaService(
        HttpRequestSender.Factory factory,
        ServiceComponents serviceComponents,
        InferenceServiceExtension.InferenceServiceFactoryContext context
    ) {
        this(factory, serviceComponents, context.clusterService());
    }

    public LlamaService(HttpRequestSender.Factory factory, ServiceComponents serviceComponents, ClusterService clusterService) {
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
        var actionCreator = new LlamaActionCreator(getSender(), getServiceComponents());
        if (model instanceof LlamaModel llamaModel) {
            llamaModel.accept(actionCreator).execute(inputs, timeout, listener);
        } else {
            listener.onFailure(createInvalidModelException(model));
        }
    }

    @Override
    protected void validateInputType(InputType inputType, Model model, ValidationException validationException) {
        ServiceUtils.validateInputTypeIsUnspecifiedOrInternal(inputType, validationException);
    }

    /**
     * Creates a LlamaModel based on the provided parameters.
     *
     * @param inferenceId the unique identifier for the inference entity
     * @param taskType the type of task this model is designed for
     * @param serviceSettings the settings for the inference service
     * @param chunkingSettings the settings for chunking, if applicable
     * @param secretSettings the secret settings for the model, such as API keys or tokens
     * @param context the context for parsing configuration settings
     * @return a new instance of LlamaModel based on the provided parameters
     */
    protected LlamaModel createModel(
        String inferenceId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        ChunkingSettings chunkingSettings,
        Map<String, Object> secretSettings,
        ConfigurationParseContext context
    ) {
        switch (taskType) {
            case TEXT_EMBEDDING:
                return new LlamaEmbeddingsModel(inferenceId, taskType, NAME, serviceSettings, chunkingSettings, secretSettings, context);
            case CHAT_COMPLETION, COMPLETION:
                return new LlamaChatCompletionModel(inferenceId, taskType, NAME, serviceSettings, secretSettings, context);
            default:
                throw createInvalidTaskTypeException(inferenceId, NAME, taskType, context);
        }
    }

    @Override
    public Model updateModelWithEmbeddingDetails(Model model, int embeddingSize) {
        if (model instanceof LlamaEmbeddingsModel embeddingsModel) {
            var serviceSettings = embeddingsModel.getServiceSettings();
            var similarityFromModel = serviceSettings.similarity();
            var similarityToUse = similarityFromModel == null ? SimilarityMeasure.DOT_PRODUCT : similarityFromModel;

            var updatedServiceSettings = new LlamaEmbeddingsServiceSettings(
                serviceSettings.modelId(),
                serviceSettings.uri(),
                embeddingSize,
                similarityToUse,
                serviceSettings.maxInputTokens(),
                serviceSettings.rateLimitSettings()
            );

            return new LlamaEmbeddingsModel(embeddingsModel, updatedServiceSettings);
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
        if (model instanceof LlamaEmbeddingsModel == false) {
            listener.onFailure(createInvalidModelException(model));
            return;
        }

        var llamaModel = (LlamaEmbeddingsModel) model;
        var actionCreator = new LlamaActionCreator(getSender(), getServiceComponents());

        List<EmbeddingRequestChunker.BatchRequestAndListener> batchedRequests = new EmbeddingRequestChunker<>(
            inputs,
            EMBEDDING_MAX_BATCH_SIZE,
            llamaModel.getConfigurations().getChunkingSettings()
        ).batchRequestsWithListeners(listener);

        for (var request : batchedRequests) {
            var action = llamaModel.accept(actionCreator);
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
        if (model instanceof LlamaChatCompletionModel == false) {
            listener.onFailure(createInvalidModelException(model));
            return;
        }

        var llamaChatCompletionModel = (LlamaChatCompletionModel) model;
        var overriddenModel = LlamaChatCompletionModel.of(llamaChatCompletionModel, inputs.getRequest());
        var manager = new GenericRequestManager<>(
            getServiceComponents().threadPool(),
            overriddenModel,
            UNIFIED_CHAT_COMPLETION_HANDLER,
            unifiedChatInput -> new LlamaChatCompletionRequest(unifiedChatInput, overriddenModel),
            UnifiedChatInput.class
        );
        var errorMessage = LlamaActionCreator.buildErrorMessage(CHAT_COMPLETION, model.getInferenceEntityId());
        var action = new SenderExecutableAction(getSender(), manager, errorMessage);

        action.execute(inputs, timeout, listener);
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
        String modelId,
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

            LlamaModel model = createModel(
                modelId,
                taskType,
                serviceSettingsMap,
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

    @Override
    public Model parsePersistedConfigWithSecrets(
        String modelId,
        TaskType taskType,
        Map<String, Object> config,
        Map<String, Object> secrets
    ) {
        Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
        removeFromMapOrDefaultEmpty(config, ModelConfigurations.TASK_SETTINGS);
        Map<String, Object> secretSettingsMap = removeFromMapOrDefaultEmpty(secrets, ModelSecrets.SECRET_SETTINGS);

        ChunkingSettings chunkingSettings = null;
        if (TaskType.TEXT_EMBEDDING.equals(taskType)) {
            chunkingSettings = ChunkingSettingsBuilder.fromMap(removeFromMap(config, ModelConfigurations.CHUNKING_SETTINGS));
        }

        return createModelFromPersistent(modelId, taskType, serviceSettingsMap, chunkingSettings, secretSettingsMap);
    }

    private LlamaModel createModelFromPersistent(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        ChunkingSettings chunkingSettings,
        Map<String, Object> secretSettings
    ) {
        return createModel(
            inferenceEntityId,
            taskType,
            serviceSettings,
            chunkingSettings,
            secretSettings,
            ConfigurationParseContext.PERSISTENT
        );
    }

    @Override
    public Model parsePersistedConfig(String modelId, TaskType taskType, Map<String, Object> config) {
        Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
        removeFromMapOrDefaultEmpty(config, ModelConfigurations.TASK_SETTINGS);

        ChunkingSettings chunkingSettings = null;
        if (TaskType.TEXT_EMBEDDING.equals(taskType)) {
            chunkingSettings = ChunkingSettingsBuilder.fromMap(removeFromMap(config, ModelConfigurations.CHUNKING_SETTINGS));
        }

        return createModelFromPersistent(modelId, taskType, serviceSettingsMap, chunkingSettings, null);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return ML_INFERENCE_LLAMA_ADDED;
    }

    /**
     * Configuration class for the Llama inference service.
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
                        .setRequired(true)
                        .setSensitive(false)
                        .setUpdatable(false)
                        .setType(SettingsConfigurationFieldType.STRING)
                        .build()
                );
                configurationMap.put(
                    MODEL_ID,
                    new SettingsConfiguration.Builder(SUPPORTED_TASK_TYPES).setDescription(
                        "Refer to the Llama models documentation for the list of available models."
                    )
                        .setLabel("Model")
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
