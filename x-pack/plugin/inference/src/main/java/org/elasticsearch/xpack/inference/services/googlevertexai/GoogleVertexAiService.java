/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.core.Nullable;
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
import org.elasticsearch.xpack.inference.services.googlevertexai.action.GoogleVertexAiActionCreator;
import org.elasticsearch.xpack.inference.services.googlevertexai.completion.GoogleVertexAiChatCompletionModel;
import org.elasticsearch.xpack.inference.services.googlevertexai.completion.GoogleVertexAiCompletionModel;
import org.elasticsearch.xpack.inference.services.googlevertexai.embeddings.GoogleVertexAiEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.googlevertexai.embeddings.GoogleVertexAiEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.googlevertexai.request.GoogleVertexAiUnifiedChatCompletionRequest;
import org.elasticsearch.xpack.inference.services.googlevertexai.rerank.GoogleVertexAiRerankModel;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createInvalidModelException;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.parsePersistedConfigErrorMsg;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMap;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrDefaultEmpty;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrThrowIfNull;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.throwIfNotEmptyMap;
import static org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiServiceFields.EMBEDDING_MAX_BATCH_SIZE;
import static org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiServiceFields.LOCATION;
import static org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiServiceFields.PROJECT_ID;
import static org.elasticsearch.xpack.inference.services.googlevertexai.action.GoogleVertexAiActionCreator.COMPLETION_ERROR_PREFIX;

public class GoogleVertexAiService extends SenderService {

    public static final String NAME = "googlevertexai";

    private static final String SERVICE_NAME = "Google Vertex AI";
    private static final EnumSet<TaskType> supportedTaskTypes = EnumSet.of(
        TaskType.TEXT_EMBEDDING,
        TaskType.RERANK,
        TaskType.CHAT_COMPLETION,
        TaskType.COMPLETION
    );

    public static final EnumSet<InputType> VALID_INPUT_TYPE_VALUES = EnumSet.of(
        InputType.INGEST,
        InputType.SEARCH,
        InputType.CLASSIFICATION,
        InputType.CLUSTERING,
        InputType.INTERNAL_INGEST,
        InputType.INTERNAL_SEARCH
    );

    private final ResponseHandler COMPLETION_HANDLER = new GoogleVertexAiUnifiedChatCompletionResponseHandler(
        "Google VertexAI chat completion"
    );

    @Override
    public Set<TaskType> supportedStreamingTasks() {
        return EnumSet.of(TaskType.CHAT_COMPLETION, TaskType.COMPLETION);
    }

    public GoogleVertexAiService(HttpRequestSender.Factory factory, ServiceComponents serviceComponents) {
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
        ActionListener<Model> parseModelListener
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

            GoogleVertexAiModel model = createModel(
                inferenceEntityId,
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

            parseModelListener.onResponse(model);
        } catch (Exception e) {
            parseModelListener.onFailure(e);
        }
    }

    @Override
    public Model parsePersistedConfigWithSecrets(
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

        return createModelFromPersistent(
            inferenceEntityId,
            taskType,
            serviceSettingsMap,
            taskSettingsMap,
            chunkingSettings,
            secretSettingsMap,
            parsePersistedConfigErrorMsg(inferenceEntityId, NAME)
        );
    }

    @Override
    public Model parsePersistedConfig(String inferenceEntityId, TaskType taskType, Map<String, Object> config) {
        Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
        Map<String, Object> taskSettingsMap = removeFromMapOrDefaultEmpty(config, ModelConfigurations.TASK_SETTINGS);

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
            null,
            parsePersistedConfigErrorMsg(inferenceEntityId, NAME)
        );
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
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_15_0;
    }

    @Override
    protected void doInfer(
        Model model,
        InferenceInputs inputs,
        Map<String, Object> taskSettings,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        if (model instanceof GoogleVertexAiModel == false) {
            listener.onFailure(createInvalidModelException(model));
            return;
        }

        GoogleVertexAiModel googleVertexAiModel = (GoogleVertexAiModel) model;

        var actionCreator = new GoogleVertexAiActionCreator(getSender(), getServiceComponents());

        var action = googleVertexAiModel.accept(actionCreator, taskSettings);
        action.execute(inputs, timeout, listener);
    }

    @Override
    protected void validateInputType(InputType inputType, Model model, ValidationException validationException) {
        ServiceUtils.validateInputTypeAgainstAllowlist(inputType, VALID_INPUT_TYPE_VALUES, SERVICE_NAME, validationException);
    }

    @Override
    protected void doUnifiedCompletionInfer(
        Model model,
        UnifiedChatInput inputs,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        if (model instanceof GoogleVertexAiChatCompletionModel == false) {
            listener.onFailure(createInvalidModelException(model));
            return;
        }
        var chatCompletionModel = (GoogleVertexAiChatCompletionModel) model;
        var updatedChatCompletionModel = GoogleVertexAiChatCompletionModel.of(chatCompletionModel, inputs.getRequest());

        var manager = new GenericRequestManager<>(
            getServiceComponents().threadPool(),
            updatedChatCompletionModel,
            COMPLETION_HANDLER,
            (unifiedChatInput) -> new GoogleVertexAiUnifiedChatCompletionRequest(unifiedChatInput, updatedChatCompletionModel),
            UnifiedChatInput.class
        );

        var errorMessage = constructFailedToSendRequestMessage(COMPLETION_ERROR_PREFIX);
        var action = new SenderExecutableAction(getSender(), manager, errorMessage);
        action.execute(inputs, timeout, listener);
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
        GoogleVertexAiModel googleVertexAiModel = (GoogleVertexAiModel) model;
        var actionCreator = new GoogleVertexAiActionCreator(getSender(), getServiceComponents());

        List<EmbeddingRequestChunker.BatchRequestAndListener> batchedRequests = new EmbeddingRequestChunker<>(
            inputs.getInputs(),
            EMBEDDING_MAX_BATCH_SIZE,
            googleVertexAiModel.getConfigurations().getChunkingSettings()
        ).batchRequestsWithListeners(listener);

        for (var request : batchedRequests) {
            var action = googleVertexAiModel.accept(actionCreator, taskSettings);
            action.execute(EmbeddingsInput.fromStrings(request.batch().inputs().get(), inputType), timeout, request.listener());
        }
    }

    @Override
    public Model updateModelWithEmbeddingDetails(Model model, int embeddingSize) {
        if (model instanceof GoogleVertexAiEmbeddingsModel embeddingsModel) {
            var serviceSettings = embeddingsModel.getServiceSettings();

            var updatedServiceSettings = new GoogleVertexAiEmbeddingsServiceSettings(
                serviceSettings.location(),
                serviceSettings.projectId(),
                serviceSettings.modelId(),
                serviceSettings.dimensionsSetByUser(),
                serviceSettings.maxInputTokens(),
                embeddingSize,
                serviceSettings.similarity(),
                serviceSettings.rateLimitSettings()
            );

            return new GoogleVertexAiEmbeddingsModel(embeddingsModel, updatedServiceSettings);
        } else {
            throw ServiceUtils.invalidModelTypeForUpdateModelWithEmbeddingDetails(model.getClass());
        }
    }

    private static GoogleVertexAiModel createModelFromPersistent(
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

    private static GoogleVertexAiModel createModel(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        ChunkingSettings chunkingSettings,
        @Nullable Map<String, Object> secretSettings,
        String failureMessage,
        ConfigurationParseContext context
    ) {
        return switch (taskType) {
            case TEXT_EMBEDDING -> new GoogleVertexAiEmbeddingsModel(
                inferenceEntityId,
                taskType,
                NAME,
                serviceSettings,
                taskSettings,
                chunkingSettings,
                secretSettings,
                context
            );
            case RERANK -> new GoogleVertexAiRerankModel(
                inferenceEntityId,
                taskType,
                NAME,
                serviceSettings,
                taskSettings,
                secretSettings,
                context
            );

            case CHAT_COMPLETION -> new GoogleVertexAiChatCompletionModel(
                inferenceEntityId,
                taskType,
                NAME,
                serviceSettings,
                taskSettings,
                secretSettings,
                context
            );

            case COMPLETION -> new GoogleVertexAiCompletionModel(
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

    public static class Configuration {
        public static InferenceServiceConfiguration get() {
            return configuration.getOrCompute();
        }

        private static final LazyInitializable<InferenceServiceConfiguration, RuntimeException> configuration = new LazyInitializable<>(
            () -> {
                var configurationMap = new HashMap<String, SettingsConfiguration>();

                // TODO whether the model ID is required or not depends on the task type
                // For rerank it is optional, for text_embedding it is required
                configurationMap.put(
                    MODEL_ID,
                    new SettingsConfiguration.Builder(supportedTaskTypes).setDescription("ID of the LLM you're using.")
                        .setLabel("Model ID")
                        .setRequired(true)
                        .setSensitive(false)
                        .setUpdatable(false)
                        .setType(SettingsConfigurationFieldType.STRING)
                        .build()
                );

                configurationMap.put(
                    LOCATION,
                    new SettingsConfiguration.Builder(EnumSet.of(TaskType.TEXT_EMBEDDING, TaskType.CHAT_COMPLETION, TaskType.COMPLETION))
                        .setDescription(
                            "Please provide the GCP region where the Vertex AI API(s) is enabled. "
                                + "For more information, refer to the {geminiVertexAIDocs}."
                        )
                        .setLabel("GCP Region")
                        .setRequired(true)
                        .setSensitive(false)
                        .setUpdatable(false)
                        .setType(SettingsConfigurationFieldType.STRING)
                        .build()
                );

                configurationMap.put(
                    PROJECT_ID,
                    new SettingsConfiguration.Builder(supportedTaskTypes).setDescription(
                        "The GCP Project ID which has Vertex AI API(s) enabled. For more information "
                            + "on the URL, refer to the {geminiVertexAIDocs}."
                    )
                        .setLabel("GCP Project")
                        .setRequired(true)
                        .setSensitive(false)
                        .setUpdatable(false)
                        .setType(SettingsConfigurationFieldType.STRING)
                        .build()
                );

                configurationMap.putAll(GoogleVertexAiSecretSettings.Configuration.get());
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
