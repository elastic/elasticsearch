/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openshiftai;

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
import org.elasticsearch.xpack.inference.services.openai.response.OpenAiChatCompletionResponseEntity;
import org.elasticsearch.xpack.inference.services.openshiftai.action.OpenShiftAiActionCreator;
import org.elasticsearch.xpack.inference.services.openshiftai.completion.OpenShiftAiChatCompletionModel;
import org.elasticsearch.xpack.inference.services.openshiftai.completion.OpenShiftAiChatCompletionResponseHandler;
import org.elasticsearch.xpack.inference.services.openshiftai.embeddings.OpenShiftAiEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.openshiftai.embeddings.OpenShiftAiEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.openshiftai.request.completion.OpenShiftAiChatCompletionRequest;
import org.elasticsearch.xpack.inference.services.openshiftai.rerank.OpenShiftAiRerankModel;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createInvalidModelException;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createInvalidTaskTypeException;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrDefaultEmpty;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrThrowIfNull;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.throwIfNotEmptyMap;

/**
 * OpenShiftAiService is an implementation of the {@link SenderService} and {@link RerankingInferenceService} that handles inference tasks
 * using models deployed to OpenShift AI environment.
 * The service uses {@link OpenShiftAiActionCreator} to create actions for executing inference requests.
 */
public class OpenShiftAiService extends SenderService implements RerankingInferenceService {
    public static final String NAME = "openshift_ai";
    /**
     * The optimal batch size depends on the model deployed in OpenShift AI.
     * For OpenShift AI use a conservatively small max batch size as it is unknown what model is deployed.
     */
    static final int EMBEDDING_MAX_BATCH_SIZE = 20;
    private static final String SERVICE_NAME = "OpenShift AI";
    private static final EnumSet<TaskType> SUPPORTED_TASK_TYPES = EnumSet.of(
        TaskType.TEXT_EMBEDDING,
        TaskType.COMPLETION,
        TaskType.CHAT_COMPLETION,
        TaskType.RERANK
    );
    private static final ResponseHandler CHAT_COMPLETION_HANDLER = new OpenShiftAiChatCompletionResponseHandler(
        "OpenShift AI chat completions",
        OpenAiChatCompletionResponseEntity::fromResponse
    );

    public OpenShiftAiService(
        HttpRequestSender.Factory factory,
        ServiceComponents serviceComponents,
        InferenceServiceExtension.InferenceServiceFactoryContext context
    ) {
        this(factory, serviceComponents, context.clusterService());
    }

    public OpenShiftAiService(HttpRequestSender.Factory factory, ServiceComponents serviceComponents, ClusterService clusterService) {
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
        if (model instanceof OpenShiftAiModel == false) {
            listener.onFailure(createInvalidModelException(model));
            return;
        }
        var openShiftAiModel = (OpenShiftAiModel) model;
        var actionCreator = new OpenShiftAiActionCreator(getSender(), getServiceComponents());
        openShiftAiModel.accept(actionCreator, taskSettings).execute(inputs, timeout, listener);
    }

    @Override
    protected void validateInputType(InputType inputType, Model model, ValidationException validationException) {
        ServiceUtils.validateInputTypeIsUnspecifiedOrInternal(inputType, validationException);
    }

    @Override
    protected void doUnifiedCompletionInfer(
        Model model,
        UnifiedChatInput inputs,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        if (model instanceof OpenShiftAiChatCompletionModel == false) {
            listener.onFailure(createInvalidModelException(model));
            return;
        }

        OpenShiftAiChatCompletionModel chatCompletionModel = (OpenShiftAiChatCompletionModel) model;
        var overriddenModel = OpenShiftAiChatCompletionModel.of(chatCompletionModel, inputs.getRequest().model());
        var manager = new GenericRequestManager<>(
            getServiceComponents().threadPool(),
            overriddenModel,
            CHAT_COMPLETION_HANDLER,
            unifiedChatInput -> new OpenShiftAiChatCompletionRequest(unifiedChatInput, overriddenModel),
            UnifiedChatInput.class
        );
        var errorMessage = OpenShiftAiActionCreator.buildErrorMessage(TaskType.CHAT_COMPLETION, model.getInferenceEntityId());
        var action = new SenderExecutableAction(getSender(), manager, errorMessage);

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
        if (model instanceof OpenShiftAiEmbeddingsModel == false) {
            listener.onFailure(createInvalidModelException(model));
            return;
        }
        var openShiftAiEmbeddingsModel = (OpenShiftAiEmbeddingsModel) model;
        var actionCreator = new OpenShiftAiActionCreator(getSender(), getServiceComponents());
        List<EmbeddingRequestChunker.BatchRequestAndListener> batchedRequests = new EmbeddingRequestChunker<>(
            inputs,
            EMBEDDING_MAX_BATCH_SIZE,
            openShiftAiEmbeddingsModel.getConfigurations().getChunkingSettings()
        ).batchRequestsWithListeners(listener);

        for (var request : batchedRequests) {
            var action = openShiftAiEmbeddingsModel.accept(actionCreator, taskSettings);
            action.execute(new EmbeddingsInput(request.batch().inputs(), inputType), timeout, request.listener());
        }
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

            OpenShiftAiModel model = createModel(
                inferenceEntityId,
                taskType,
                serviceSettingsMap,
                serviceSettingsMap,
                taskSettingsMap,
                chunkingSettings,
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
    public OpenShiftAiModel parsePersistedConfigWithSecrets(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> config,
        Map<String, Object> secrets
    ) {
        Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
        ChunkingSettings chunkingSettings = null;
        if (TaskType.TEXT_EMBEDDING.equals(taskType)) {
            chunkingSettings = ChunkingSettingsBuilder.fromMap(removeFromMapOrDefaultEmpty(config, ModelConfigurations.CHUNKING_SETTINGS));
        }
        Map<String, Object> taskSettingsMap = removeFromMapOrDefaultEmpty(config, ModelConfigurations.TASK_SETTINGS);
        Map<String, Object> secretSettingsMap = removeFromMapOrDefaultEmpty(secrets, ModelSecrets.SECRET_SETTINGS);

        return createModelFromPersistent(
            inferenceEntityId,
            taskType,
            serviceSettingsMap,
            secretSettingsMap,
            taskSettingsMap,
            chunkingSettings
        );
    }

    @Override
    public OpenShiftAiModel parsePersistedConfig(String inferenceEntityId, TaskType taskType, Map<String, Object> config) {
        Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
        ChunkingSettings chunkingSettingsMap = null;
        if (TaskType.TEXT_EMBEDDING.equals(taskType)) {
            chunkingSettingsMap = ChunkingSettingsBuilder.fromMap(
                removeFromMapOrDefaultEmpty(config, ModelConfigurations.CHUNKING_SETTINGS)
            );
        }
        Map<String, Object> taskSettingsMap = removeFromMapOrDefaultEmpty(config, ModelConfigurations.TASK_SETTINGS);

        return createModelFromPersistent(inferenceEntityId, taskType, serviceSettingsMap, null, taskSettingsMap, chunkingSettingsMap);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return OpenShiftAiUtils.ML_INFERENCE_OPENSHIFT_AI_ADDED;
    }

    @Override
    public Set<TaskType> supportedStreamingTasks() {
        return EnumSet.of(TaskType.COMPLETION, TaskType.CHAT_COMPLETION);
    }

    private static OpenShiftAiModel createModel(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        @Nullable Map<String, Object> secretSettings,
        Map<String, Object> taskSettings,
        ChunkingSettings chunkingSettings,
        ConfigurationParseContext context
    ) {
        return switch (taskType) {
            case CHAT_COMPLETION, COMPLETION -> new OpenShiftAiChatCompletionModel(
                inferenceEntityId,
                taskType,
                NAME,
                serviceSettings,
                secretSettings,
                context
            );
            case TEXT_EMBEDDING -> new OpenShiftAiEmbeddingsModel(
                inferenceEntityId,
                taskType,
                NAME,
                serviceSettings,
                chunkingSettings,
                secretSettings,
                context
            );
            case RERANK -> new OpenShiftAiRerankModel(
                inferenceEntityId,
                taskType,
                NAME,
                serviceSettings,
                taskSettings,
                secretSettings,
                context
            );
            default -> throw createInvalidTaskTypeException(inferenceEntityId, NAME, taskType, context);
        };
    }

    private OpenShiftAiModel createModelFromPersistent(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        Map<String, Object> secretSettings,
        Map<String, Object> taskSettings,
        ChunkingSettings chunkingSettings
    ) {
        return createModel(
            inferenceEntityId,
            taskType,
            serviceSettings,
            secretSettings,
            taskSettings,
            chunkingSettings,
            ConfigurationParseContext.PERSISTENT
        );
    }

    @Override
    public int rerankerWindowSize(String modelId) {
        // OpenShift AI uses Cohere and JinaAI rerank protocols for reranking
        // JinaAI rerank model has 131K tokens limit https://jina.ai/models/jina-reranker-v3/
        // Cohere rerank model truncates at 4096 tokens https://docs.cohere.com/reference/rerank
        // We choose a conservative limit based on these two models
        // Using 1 token = 0.75 words as a rough estimate, we get 3072 words allowing for some headroom, we set the window size below 3072
        return 2800;
    }

    @Override
    public Model updateModelWithEmbeddingDetails(Model model, int embeddingSize) {
        if (model instanceof OpenShiftAiEmbeddingsModel embeddingsModel) {
            var serviceSettings = embeddingsModel.getServiceSettings();
            var similarityFromModel = serviceSettings.similarity();
            var similarityToUse = similarityFromModel == null ? SimilarityMeasure.DOT_PRODUCT : similarityFromModel;

            var updatedServiceSettings = new OpenShiftAiEmbeddingsServiceSettings(
                serviceSettings.modelId(),
                serviceSettings.uri(),
                embeddingSize,
                similarityToUse,
                serviceSettings.maxInputTokens(),
                serviceSettings.rateLimitSettings(),
                serviceSettings.dimensionsSetByUser()
            );

            return new OpenShiftAiEmbeddingsModel(embeddingsModel, updatedServiceSettings);
        } else {
            throw ServiceUtils.invalidModelTypeForUpdateModelWithEmbeddingDetails(model.getClass());
        }
    }

    /**
     * Configuration class for the OpenShift AI inference service.
     * It provides the settings and configurations required for the service.
     */
    public static class Configuration {
        public static InferenceServiceConfiguration get() {
            return CONFIGURATION.getOrCompute();
        }

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
                        "The name of the model to use for the inference task."
                    )
                        .setLabel("Model ID")
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
