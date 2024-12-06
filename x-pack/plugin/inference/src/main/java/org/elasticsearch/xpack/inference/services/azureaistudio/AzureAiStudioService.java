/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaistudio;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkedInferenceServiceResults;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.EmptySettingsConfiguration;
import org.elasticsearch.inference.InferenceServiceConfiguration;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.SettingsConfiguration;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskSettingsConfiguration;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.configuration.SettingsConfigurationDisplayType;
import org.elasticsearch.inference.configuration.SettingsConfigurationFieldType;
import org.elasticsearch.inference.configuration.SettingsConfigurationSelectOption;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.inference.chunking.ChunkingSettingsBuilder;
import org.elasticsearch.xpack.inference.chunking.EmbeddingRequestChunker;
import org.elasticsearch.xpack.inference.external.action.azureaistudio.AzureAiStudioActionCreator;
import org.elasticsearch.xpack.inference.external.http.sender.DocumentsOnlyInput;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.SenderService;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.azureaistudio.completion.AzureAiStudioChatCompletionModel;
import org.elasticsearch.xpack.inference.services.azureaistudio.completion.AzureAiStudioChatCompletionTaskSettings;
import org.elasticsearch.xpack.inference.services.azureaistudio.embeddings.AzureAiStudioEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.azureaistudio.embeddings.AzureAiStudioEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.validation.ModelValidatorBuilder;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.createInvalidModelException;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.parsePersistedConfigErrorMsg;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMap;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrDefaultEmpty;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrThrowIfNull;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.throwIfNotEmptyMap;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.ENDPOINT_TYPE_FIELD;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.PROVIDER_FIELD;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.TARGET_FIELD;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioProviderCapabilities.providerAllowsEndpointTypeForTask;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioProviderCapabilities.providerAllowsTaskType;
import static org.elasticsearch.xpack.inference.services.azureaistudio.completion.AzureAiStudioChatCompletionTaskSettings.DEFAULT_MAX_NEW_TOKENS;
import static org.elasticsearch.xpack.inference.services.openai.OpenAiServiceFields.EMBEDDING_MAX_BATCH_SIZE;

public class AzureAiStudioService extends SenderService {

    static final String NAME = "azureaistudio";

    private static final EnumSet<TaskType> supportedTaskTypes = EnumSet.of(TaskType.TEXT_EMBEDDING, TaskType.COMPLETION);

    public AzureAiStudioService(HttpRequestSender.Factory factory, ServiceComponents serviceComponents) {
        super(factory, serviceComponents);
    }

    @Override
    protected void doInfer(
        Model model,
        InferenceInputs inputs,
        Map<String, Object> taskSettings,
        InputType inputType,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        var actionCreator = new AzureAiStudioActionCreator(getSender(), getServiceComponents());

        if (model instanceof AzureAiStudioModel baseAzureAiStudioModel) {
            var action = baseAzureAiStudioModel.accept(actionCreator, taskSettings);
            action.execute(inputs, timeout, listener);
        } else {
            listener.onFailure(createInvalidModelException(model));
        }
    }

    @Override
    protected void doChunkedInfer(
        Model model,
        DocumentsOnlyInput inputs,
        Map<String, Object> taskSettings,
        InputType inputType,
        TimeValue timeout,
        ActionListener<List<ChunkedInferenceServiceResults>> listener
    ) {
        if (model instanceof AzureAiStudioModel baseAzureAiStudioModel) {
            var actionCreator = new AzureAiStudioActionCreator(getSender(), getServiceComponents());

            List<EmbeddingRequestChunker.BatchRequestAndListener> batchedRequests = new EmbeddingRequestChunker(
                inputs.getInputs(),
                EMBEDDING_MAX_BATCH_SIZE,
                EmbeddingRequestChunker.EmbeddingType.FLOAT,
                baseAzureAiStudioModel.getConfigurations().getChunkingSettings()
            ).batchRequestsWithListeners(listener);

            for (var request : batchedRequests) {
                var action = baseAzureAiStudioModel.accept(actionCreator, taskSettings);
                action.execute(new DocumentsOnlyInput(request.batch().inputs()), timeout, request.listener());
            }
        } else {
            listener.onFailure(createInvalidModelException(model));
        }
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

            AzureAiStudioModel model = createModel(
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

            parsedModelListener.onResponse(model);
        } catch (Exception e) {
            parsedModelListener.onFailure(e);
        }
    }

    @Override
    public AzureAiStudioModel parsePersistedConfigWithSecrets(
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
    public String name() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_15_0;
    }

    @Override
    public Set<TaskType> supportedStreamingTasks() {
        return COMPLETION_ONLY;
    }

    private static AzureAiStudioModel createModel(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        ChunkingSettings chunkingSettings,
        @Nullable Map<String, Object> secretSettings,
        String failureMessage,
        ConfigurationParseContext context
    ) {

        if (taskType == TaskType.TEXT_EMBEDDING) {
            var embeddingsModel = new AzureAiStudioEmbeddingsModel(
                inferenceEntityId,
                taskType,
                NAME,
                serviceSettings,
                taskSettings,
                chunkingSettings,
                secretSettings,
                context
            );
            checkProviderAndEndpointTypeForTask(
                TaskType.TEXT_EMBEDDING,
                embeddingsModel.getServiceSettings().provider(),
                embeddingsModel.getServiceSettings().endpointType()
            );
            return embeddingsModel;
        }

        if (taskType == TaskType.COMPLETION) {
            var completionModel = new AzureAiStudioChatCompletionModel(
                inferenceEntityId,
                taskType,
                NAME,
                serviceSettings,
                taskSettings,
                secretSettings,
                context
            );
            checkProviderAndEndpointTypeForTask(
                TaskType.COMPLETION,
                completionModel.getServiceSettings().provider(),
                completionModel.getServiceSettings().endpointType()
            );
            return completionModel;
        }

        throw new ElasticsearchStatusException(failureMessage, RestStatus.BAD_REQUEST);
    }

    private AzureAiStudioModel createModelFromPersistent(
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
    public void checkModelConfig(Model model, ActionListener<Model> listener) {
        // TODO: Remove this function once all services have been updated to use the new model validators
        ModelValidatorBuilder.buildModelValidator(model.getTaskType()).validate(this, model, listener);
    }

    @Override
    public Model updateModelWithEmbeddingDetails(Model model, int embeddingSize) {
        if (model instanceof AzureAiStudioEmbeddingsModel embeddingsModel) {
            var serviceSettings = embeddingsModel.getServiceSettings();
            var similarityFromModel = serviceSettings.similarity();
            var similarityToUse = similarityFromModel == null ? SimilarityMeasure.DOT_PRODUCT : similarityFromModel;

            var updatedServiceSettings = new AzureAiStudioEmbeddingsServiceSettings(
                serviceSettings.target(),
                serviceSettings.provider(),
                serviceSettings.endpointType(),
                embeddingSize,
                serviceSettings.dimensionsSetByUser(),
                serviceSettings.maxInputTokens(),
                similarityToUse,
                serviceSettings.rateLimitSettings()
            );

            return new AzureAiStudioEmbeddingsModel(embeddingsModel, updatedServiceSettings);
        } else {
            throw ServiceUtils.invalidModelTypeForUpdateModelWithEmbeddingDetails(model.getClass());
        }
    }

    @Override
    public Model updateModelWithChatCompletionDetails(Model model) {
        if (model instanceof AzureAiStudioChatCompletionModel chatCompletionModel) {
            var taskSettings = chatCompletionModel.getTaskSettings();
            var modelMaxNewTokens = taskSettings.maxNewTokens();
            var maxNewTokensToUse = modelMaxNewTokens == null ? DEFAULT_MAX_NEW_TOKENS : modelMaxNewTokens;

            var updatedTaskSettings = new AzureAiStudioChatCompletionTaskSettings(
                taskSettings.temperature(),
                taskSettings.topP(),
                taskSettings.doSample(),
                maxNewTokensToUse
            );

            return new AzureAiStudioChatCompletionModel(chatCompletionModel, updatedTaskSettings);
        } else {
            throw ServiceUtils.invalidModelTypeForUpdateModelWithChatCompletionDetails(model.getClass());
        }
    }

    private static void checkProviderAndEndpointTypeForTask(
        TaskType taskType,
        AzureAiStudioProvider provider,
        AzureAiStudioEndpointType endpointType
    ) {
        if (providerAllowsTaskType(provider, taskType) == false) {
            throw new ElasticsearchStatusException(
                Strings.format("The [%s] task type for provider [%s] is not available", taskType, provider),
                RestStatus.BAD_REQUEST
            );
        }

        if (providerAllowsEndpointTypeForTask(provider, taskType, endpointType) == false) {
            throw new ElasticsearchStatusException(
                Strings.format(
                    "The [%s] endpoint type with [%s] task type for provider [%s] is not available",
                    endpointType,
                    taskType,
                    provider
                ),
                RestStatus.BAD_REQUEST
            );
        }
    }

    public static class Configuration {
        public static InferenceServiceConfiguration get() {
            return configuration.getOrCompute();
        }

        private static final LazyInitializable<InferenceServiceConfiguration, RuntimeException> configuration = new LazyInitializable<>(
            () -> {
                var configurationMap = new HashMap<String, SettingsConfiguration>();

                configurationMap.put(
                    TARGET_FIELD,
                    new SettingsConfiguration.Builder().setDisplay(SettingsConfigurationDisplayType.TEXTBOX)
                        .setLabel("Target")
                        .setOrder(2)
                        .setRequired(true)
                        .setSensitive(false)
                        .setTooltip("The target URL of your Azure AI Studio model deployment.")
                        .setType(SettingsConfigurationFieldType.STRING)
                        .build()
                );

                configurationMap.put(
                    ENDPOINT_TYPE_FIELD,
                    new SettingsConfiguration.Builder().setDisplay(SettingsConfigurationDisplayType.DROPDOWN)
                        .setLabel("Endpoint Type")
                        .setOrder(3)
                        .setRequired(true)
                        .setSensitive(false)
                        .setTooltip("Specifies the type of endpoint that is used in your model deployment.")
                        .setType(SettingsConfigurationFieldType.STRING)
                        .setOptions(
                            Stream.of("token", "realtime")
                                .map(v -> new SettingsConfigurationSelectOption.Builder().setLabelAndValue(v).build())
                                .toList()
                        )
                        .build()
                );

                configurationMap.put(
                    PROVIDER_FIELD,
                    new SettingsConfiguration.Builder().setDisplay(SettingsConfigurationDisplayType.DROPDOWN)
                        .setLabel("Provider")
                        .setOrder(3)
                        .setRequired(true)
                        .setSensitive(false)
                        .setTooltip("The model provider for your deployment.")
                        .setType(SettingsConfigurationFieldType.STRING)
                        .setOptions(
                            Stream.of("cohere", "meta", "microsoft_phi", "mistral", "openai", "databricks")
                                .map(v -> new SettingsConfigurationSelectOption.Builder().setLabelAndValue(v).build())
                                .toList()
                        )
                        .build()
                );

                configurationMap.putAll(DefaultSecretSettings.toSettingsConfiguration());
                configurationMap.putAll(RateLimitSettings.toSettingsConfiguration());

                return new InferenceServiceConfiguration.Builder().setProvider(NAME).setTaskTypes(supportedTaskTypes.stream().map(t -> {
                    Map<String, SettingsConfiguration> taskSettingsConfig;
                    switch (t) {
                        case TEXT_EMBEDDING -> taskSettingsConfig = AzureAiStudioEmbeddingsModel.Configuration.get();
                        case COMPLETION -> taskSettingsConfig = AzureAiStudioChatCompletionModel.Configuration.get();
                        default -> taskSettingsConfig = EmptySettingsConfiguration.get();
                    }
                    return new TaskSettingsConfiguration.Builder().setTaskType(t).setConfiguration(taskSettingsConfig).build();
                }).toList()).setConfiguration(configurationMap).build();
            }
        );
    }
}
