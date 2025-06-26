/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.llama;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.InferenceServiceConfiguration;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.SettingsConfiguration;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.configuration.SettingsConfigurationFieldType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.inference.chunking.ChunkingSettingsBuilder;
import org.elasticsearch.xpack.inference.chunking.EmbeddingRequestChunker;
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
import static org.elasticsearch.xpack.inference.services.ServiceUtils.parsePersistedConfigErrorMsg;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMap;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrDefaultEmpty;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrThrowIfNull;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.throwIfNotEmptyMap;

public class LlamaService extends SenderService {
    public static final String NAME = "llama";

    private static final String SERVICE_NAME = "Llama";
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

    public LlamaService(HttpRequestSender.Factory factory, ServiceComponents serviceComponents) {
        super(factory, serviceComponents);
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

        if (model instanceof LlamaEmbeddingsModel llamaEmbeddingsModel) {
            llamaEmbeddingsModel.accept(actionCreator).execute(inputs, timeout, listener);
        } else if (model instanceof LlamaChatCompletionModel llamaChatCompletionModel) {
            llamaChatCompletionModel.accept(actionCreator).execute(inputs, timeout, listener);
        } else {
            listener.onFailure(createInvalidModelException(model));
        }
    }

    @Override
    protected void validateInputType(InputType inputType, Model model, ValidationException validationException) {
        ServiceUtils.validateInputTypeIsUnspecifiedOrInternal(inputType, validationException);
    }

    protected LlamaModel createModel(
        String inferenceId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        ChunkingSettings chunkingSettings,
        Map<String, Object> secretSettings,
        String failureMessage,
        ConfigurationParseContext context
    ) {
        switch (taskType) {
            case TEXT_EMBEDDING:
                return new LlamaEmbeddingsModel(
                    inferenceId,
                    taskType,
                    NAME,
                    serviceSettings,
                    taskSettings,
                    chunkingSettings,
                    secretSettings,
                    context
                );
            case CHAT_COMPLETION, COMPLETION:
                return new LlamaChatCompletionModel(inferenceId, taskType, NAME, serviceSettings, secretSettings, context);
            default:
                throw new ElasticsearchStatusException(failureMessage, RestStatus.BAD_REQUEST);
        }
    }

    @Override
    public Model updateModelWithEmbeddingDetails(Model model, int embeddingSize) {
        if (model instanceof LlamaEmbeddingsModel embeddingsModel) {
            var serviceSettings = embeddingsModel.getServiceSettings();
            var similarityFromModel = serviceSettings.similarity();
            var similarityToUse = similarityFromModel == null ? SimilarityMeasure.COSINE : similarityFromModel;

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
        EmbeddingsInput inputs,
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
            inputs.getInputs(),
            EMBEDDING_MAX_BATCH_SIZE,
            llamaModel.getConfigurations().getChunkingSettings()
        ).batchRequestsWithListeners(listener);

        for (var request : batchedRequests) {
            var action = llamaModel.accept(actionCreator);
            action.execute(EmbeddingsInput.fromStrings(request.batch().inputs().get(), inputType), timeout, request.listener());
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
                taskSettingsMap,
                chunkingSettings,
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

    @Override
    public Model parsePersistedConfigWithSecrets(
        String modelId,
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

        return createModelFromPersistent(
            modelId,
            taskType,
            serviceSettingsMap,
            taskSettingsMap,
            chunkingSettings,
            secretSettingsMap,
            parsePersistedConfigErrorMsg(modelId, NAME)
        );
    }

    private LlamaModel createModelFromPersistent(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        ChunkingSettings chunkingSettings,
        Map<String, Object> secretSettings,
        String failureMessage
    ) {
        return createModel(
            inferenceEntityId,
            taskType,
            serviceSettings,
            taskSettings,
            chunkingSettings,
            secretSettings,
            failureMessage,
            ConfigurationParseContext.PERSISTENT
        );
    }

    @Override
    public Model parsePersistedConfig(String modelId, TaskType taskType, Map<String, Object> config) {
        Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
        Map<String, Object> taskSettingsMap = removeFromMapOrDefaultEmpty(config, ModelConfigurations.TASK_SETTINGS);

        ChunkingSettings chunkingSettings = null;
        if (TaskType.TEXT_EMBEDDING.equals(taskType)) {
            chunkingSettings = ChunkingSettingsBuilder.fromMap(removeFromMap(config, ModelConfigurations.CHUNKING_SETTINGS));
        }

        return createModelFromPersistent(
            modelId,
            taskType,
            serviceSettingsMap,
            taskSettingsMap,
            chunkingSettings,
            null,
            parsePersistedConfigErrorMsg(modelId, NAME)
        );
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ML_INFERENCE_LLAMA_ADDED;
    }

    public static class Configuration {
        public static InferenceServiceConfiguration get() {
            return configuration.getOrCompute();
        }

        private Configuration() {}

        private static final LazyInitializable<InferenceServiceConfiguration, RuntimeException> configuration = new LazyInitializable<>(
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
