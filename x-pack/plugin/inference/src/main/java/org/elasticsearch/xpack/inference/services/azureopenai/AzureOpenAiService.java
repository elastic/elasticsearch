/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
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
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.inference.chunking.ChunkingSettingsBuilder;
import org.elasticsearch.xpack.inference.chunking.EmbeddingRequestChunker;
import org.elasticsearch.xpack.inference.external.action.azureopenai.AzureOpenAiActionCreator;
import org.elasticsearch.xpack.inference.external.http.sender.DocumentsOnlyInput;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.SenderService;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.azureopenai.completion.AzureOpenAiCompletionModel;
import org.elasticsearch.xpack.inference.services.azureopenai.embeddings.AzureOpenAiEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.azureopenai.embeddings.AzureOpenAiEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.validation.ModelValidatorBuilder;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.createInvalidModelException;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.parsePersistedConfigErrorMsg;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMap;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrDefaultEmpty;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrThrowIfNull;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.throwIfNotEmptyMap;
import static org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiServiceFields.API_VERSION;
import static org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiServiceFields.DEPLOYMENT_ID;
import static org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiServiceFields.RESOURCE_NAME;
import static org.elasticsearch.xpack.inference.services.openai.OpenAiServiceFields.EMBEDDING_MAX_BATCH_SIZE;

public class AzureOpenAiService extends SenderService {
    public static final String NAME = "azureopenai";

    private static final EnumSet<TaskType> supportedTaskTypes = EnumSet.of(TaskType.TEXT_EMBEDDING, TaskType.COMPLETION);

    public AzureOpenAiService(HttpRequestSender.Factory factory, ServiceComponents serviceComponents) {
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

            AzureOpenAiModel model = createModel(
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

    private static AzureOpenAiModel createModelFromPersistent(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        ChunkingSettings chunkingSettings,
        @Nullable Map<String, Object> secretSettings,
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

    private static AzureOpenAiModel createModel(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        ChunkingSettings chunkingSettings,
        @Nullable Map<String, Object> secretSettings,
        String failureMessage,
        ConfigurationParseContext context
    ) {
        switch (taskType) {
            case TEXT_EMBEDDING -> {
                return new AzureOpenAiEmbeddingsModel(
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
            case COMPLETION -> {
                return new AzureOpenAiCompletionModel(
                    inferenceEntityId,
                    taskType,
                    NAME,
                    serviceSettings,
                    taskSettings,
                    secretSettings,
                    context
                );
            }
            default -> throw new ElasticsearchStatusException(failureMessage, RestStatus.BAD_REQUEST);
        }
    }

    @Override
    public AzureOpenAiModel parsePersistedConfigWithSecrets(
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
    public AzureOpenAiModel parsePersistedConfig(String inferenceEntityId, TaskType taskType, Map<String, Object> config) {
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
    protected void doInfer(
        Model model,
        InferenceInputs inputs,
        Map<String, Object> taskSettings,
        InputType inputType,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        if (model instanceof AzureOpenAiModel == false) {
            listener.onFailure(createInvalidModelException(model));
            return;
        }

        AzureOpenAiModel azureOpenAiModel = (AzureOpenAiModel) model;
        var actionCreator = new AzureOpenAiActionCreator(getSender(), getServiceComponents());

        var action = azureOpenAiModel.accept(actionCreator, taskSettings);
        action.execute(inputs, timeout, listener);
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
        if (model instanceof AzureOpenAiModel == false) {
            listener.onFailure(createInvalidModelException(model));
            return;
        }
        AzureOpenAiModel azureOpenAiModel = (AzureOpenAiModel) model;
        var actionCreator = new AzureOpenAiActionCreator(getSender(), getServiceComponents());

        List<EmbeddingRequestChunker.BatchRequestAndListener> batchedRequests = new EmbeddingRequestChunker(
            inputs.getInputs(),
            EMBEDDING_MAX_BATCH_SIZE,
            EmbeddingRequestChunker.EmbeddingType.FLOAT,
            azureOpenAiModel.getConfigurations().getChunkingSettings()
        ).batchRequestsWithListeners(listener);

        for (var request : batchedRequests) {
            var action = azureOpenAiModel.accept(actionCreator, taskSettings);
            action.execute(new DocumentsOnlyInput(request.batch().inputs()), timeout, request.listener());
        }
    }

    /**
     * For text embedding models get the embedding size and
     * update the service settings.
     *
     * @param model    The new model
     * @param listener The listener
     */
    @Override
    public void checkModelConfig(Model model, ActionListener<Model> listener) {
        // TODO: Remove this function once all services have been updated to use the new model validators
        ModelValidatorBuilder.buildModelValidator(model.getTaskType()).validate(this, model, listener);
    }

    @Override
    public Model updateModelWithEmbeddingDetails(Model model, int embeddingSize) {
        if (model instanceof AzureOpenAiEmbeddingsModel embeddingsModel) {
            var serviceSettings = embeddingsModel.getServiceSettings();
            var similarityFromModel = serviceSettings.similarity();
            var similarityToUse = similarityFromModel == null ? SimilarityMeasure.DOT_PRODUCT : similarityFromModel;

            var updatedServiceSettings = new AzureOpenAiEmbeddingsServiceSettings(
                serviceSettings.resourceName(),
                serviceSettings.deploymentId(),
                serviceSettings.apiVersion(),
                embeddingSize,
                serviceSettings.dimensionsSetByUser(),
                serviceSettings.maxInputTokens(),
                similarityToUse,
                serviceSettings.rateLimitSettings()
            );

            return new AzureOpenAiEmbeddingsModel(embeddingsModel, updatedServiceSettings);
        } else {
            throw ServiceUtils.invalidModelTypeForUpdateModelWithEmbeddingDetails(model.getClass());
        }
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_15_0;
    }

    @Override
    public Set<TaskType> supportedStreamingTasks() {
        return COMPLETION_ONLY;
    }

    public static class Configuration {
        public static InferenceServiceConfiguration get() {
            return configuration.getOrCompute();
        }

        private static final LazyInitializable<InferenceServiceConfiguration, RuntimeException> configuration = new LazyInitializable<>(
            () -> {
                var configurationMap = new HashMap<String, SettingsConfiguration>();

                configurationMap.put(
                    RESOURCE_NAME,
                    new SettingsConfiguration.Builder().setDisplay(SettingsConfigurationDisplayType.TEXTBOX)
                        .setLabel("Resource Name")
                        .setOrder(3)
                        .setRequired(true)
                        .setSensitive(false)
                        .setTooltip("The name of your Azure OpenAI resource.")
                        .setType(SettingsConfigurationFieldType.STRING)
                        .build()
                );

                configurationMap.put(
                    API_VERSION,
                    new SettingsConfiguration.Builder().setDisplay(SettingsConfigurationDisplayType.TEXTBOX)
                        .setLabel("API Version")
                        .setOrder(4)
                        .setRequired(true)
                        .setSensitive(false)
                        .setTooltip("The Azure API version ID to use.")
                        .setType(SettingsConfigurationFieldType.STRING)
                        .build()
                );

                configurationMap.put(
                    DEPLOYMENT_ID,
                    new SettingsConfiguration.Builder().setDisplay(SettingsConfigurationDisplayType.TEXTBOX)
                        .setLabel("Deployment ID")
                        .setOrder(5)
                        .setRequired(true)
                        .setSensitive(false)
                        .setTooltip("The deployment name of your deployed models.")
                        .setType(SettingsConfigurationFieldType.STRING)
                        .build()
                );

                configurationMap.putAll(AzureOpenAiSecretSettings.Configuration.get());
                configurationMap.putAll(
                    RateLimitSettings.toSettingsConfigurationWithTooltip(
                        "The azureopenai service sets a default number of requests allowed per minute depending on the task type."
                    )
                );

                return new InferenceServiceConfiguration.Builder().setProvider(NAME).setTaskTypes(supportedTaskTypes.stream().map(t -> {
                    Map<String, SettingsConfiguration> taskSettingsConfig;
                    switch (t) {
                        case TEXT_EMBEDDING -> taskSettingsConfig = AzureOpenAiEmbeddingsModel.Configuration.get();
                        case COMPLETION -> taskSettingsConfig = AzureOpenAiCompletionModel.Configuration.get();
                        default -> taskSettingsConfig = EmptySettingsConfiguration.get();
                    }
                    return new TaskSettingsConfiguration.Builder().setTaskType(t).setConfiguration(taskSettingsConfig).build();
                }).toList()).setConfiguration(configurationMap).build();
            }
        );
    }
}
