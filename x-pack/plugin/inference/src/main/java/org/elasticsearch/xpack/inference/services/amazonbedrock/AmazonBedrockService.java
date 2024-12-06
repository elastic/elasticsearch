/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.core.IOUtils;
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
import org.elasticsearch.inference.TaskSettingsConfiguration;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.configuration.SettingsConfigurationDisplayType;
import org.elasticsearch.inference.configuration.SettingsConfigurationFieldType;
import org.elasticsearch.inference.configuration.SettingsConfigurationSelectOption;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.inference.chunking.ChunkingSettingsBuilder;
import org.elasticsearch.xpack.inference.chunking.EmbeddingRequestChunker;
import org.elasticsearch.xpack.inference.external.action.amazonbedrock.AmazonBedrockActionCreator;
import org.elasticsearch.xpack.inference.external.amazonbedrock.AmazonBedrockRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.DocumentsOnlyInput;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.SenderService;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.amazonbedrock.completion.AmazonBedrockChatCompletionModel;
import org.elasticsearch.xpack.inference.services.amazonbedrock.embeddings.AmazonBedrockEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.amazonbedrock.embeddings.AmazonBedrockEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.validation.ModelValidatorBuilder;

import java.io.IOException;
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
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.MODEL_FIELD;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.PROVIDER_FIELD;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.REGION_FIELD;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.TOP_K_FIELD;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockProviderCapabilities.chatCompletionProviderHasTopKParameter;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockProviderCapabilities.getEmbeddingsMaxBatchSize;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockProviderCapabilities.getProviderDefaultSimilarityMeasure;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockProviderCapabilities.providerAllowsTaskType;

public class AmazonBedrockService extends SenderService {
    public static final String NAME = "amazonbedrock";

    private final Sender amazonBedrockSender;

    private static final EnumSet<TaskType> supportedTaskTypes = EnumSet.of(TaskType.TEXT_EMBEDDING, TaskType.COMPLETION);

    public AmazonBedrockService(
        HttpRequestSender.Factory httpSenderFactory,
        AmazonBedrockRequestSender.Factory amazonBedrockFactory,
        ServiceComponents serviceComponents
    ) {
        super(httpSenderFactory, serviceComponents);
        this.amazonBedrockSender = amazonBedrockFactory.createSender();
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
        var actionCreator = new AmazonBedrockActionCreator(amazonBedrockSender, this.getServiceComponents(), timeout);
        if (model instanceof AmazonBedrockModel baseAmazonBedrockModel) {
            var action = baseAmazonBedrockModel.accept(actionCreator, taskSettings);
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
        var actionCreator = new AmazonBedrockActionCreator(amazonBedrockSender, this.getServiceComponents(), timeout);
        if (model instanceof AmazonBedrockModel baseAmazonBedrockModel) {
            var maxBatchSize = getEmbeddingsMaxBatchSize(baseAmazonBedrockModel.provider());

            List<EmbeddingRequestChunker.BatchRequestAndListener> batchedRequests = new EmbeddingRequestChunker(
                inputs.getInputs(),
                maxBatchSize,
                EmbeddingRequestChunker.EmbeddingType.FLOAT,
                baseAmazonBedrockModel.getConfigurations().getChunkingSettings()
            ).batchRequestsWithListeners(listener);

            for (var request : batchedRequests) {
                var action = baseAmazonBedrockModel.accept(actionCreator, taskSettings);
                action.execute(new DocumentsOnlyInput(request.batch().inputs()), timeout, request.listener());
            }
        } else {
            listener.onFailure(createInvalidModelException(model));
        }
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

            AmazonBedrockModel model = createModel(
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

        return createModel(
            modelId,
            taskType,
            serviceSettingsMap,
            taskSettingsMap,
            chunkingSettings,
            secretSettingsMap,
            parsePersistedConfigErrorMsg(modelId, NAME),
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

        return createModel(
            modelId,
            taskType,
            serviceSettingsMap,
            taskSettingsMap,
            chunkingSettings,
            null,
            parsePersistedConfigErrorMsg(modelId, NAME),
            ConfigurationParseContext.PERSISTENT
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

    private static AmazonBedrockModel createModel(
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
                var model = new AmazonBedrockEmbeddingsModel(
                    inferenceEntityId,
                    taskType,
                    NAME,
                    serviceSettings,
                    taskSettings,
                    chunkingSettings,
                    secretSettings,
                    context
                );
                checkProviderForTask(TaskType.TEXT_EMBEDDING, model.provider());
                return model;
            }
            case COMPLETION -> {
                var model = new AmazonBedrockChatCompletionModel(
                    inferenceEntityId,
                    taskType,
                    NAME,
                    serviceSettings,
                    taskSettings,
                    secretSettings,
                    context
                );
                checkProviderForTask(TaskType.COMPLETION, model.provider());
                checkChatCompletionProviderForTopKParameter(model);
                return model;
            }
            default -> throw new ElasticsearchStatusException(failureMessage, RestStatus.BAD_REQUEST);
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

    /**
     * For text embedding models get the embedding size and
     * update the service settings.
     *
     * @param model The new model
     * @param listener The listener
     */
    @Override
    public void checkModelConfig(Model model, ActionListener<Model> listener) {
        // TODO: Remove this function once all services have been updated to use the new model validators
        ModelValidatorBuilder.buildModelValidator(model.getTaskType()).validate(this, model, listener);
    }

    @Override
    public Model updateModelWithEmbeddingDetails(Model model, int embeddingSize) {
        if (model instanceof AmazonBedrockEmbeddingsModel embeddingsModel) {
            var serviceSettings = embeddingsModel.getServiceSettings();
            var similarityFromModel = serviceSettings.similarity();
            var similarityToUse = similarityFromModel == null
                ? getProviderDefaultSimilarityMeasure(embeddingsModel.provider())
                : similarityFromModel;

            var updatedServiceSettings = new AmazonBedrockEmbeddingsServiceSettings(
                serviceSettings.region(),
                serviceSettings.modelId(),
                serviceSettings.provider(),
                embeddingSize,
                serviceSettings.dimensionsSetByUser(),
                serviceSettings.maxInputTokens(),
                similarityToUse,
                serviceSettings.rateLimitSettings()
            );

            return new AmazonBedrockEmbeddingsModel(embeddingsModel, updatedServiceSettings);
        } else {
            throw ServiceUtils.invalidModelTypeForUpdateModelWithEmbeddingDetails(model.getClass());
        }
    }

    private static void checkProviderForTask(TaskType taskType, AmazonBedrockProvider provider) {
        if (providerAllowsTaskType(provider, taskType) == false) {
            throw new ElasticsearchStatusException(
                Strings.format("The [%s] task type for provider [%s] is not available", taskType, provider),
                RestStatus.BAD_REQUEST
            );
        }
    }

    private static void checkChatCompletionProviderForTopKParameter(AmazonBedrockChatCompletionModel model) {
        var taskSettings = model.getTaskSettings();
        if (taskSettings.topK() != null) {
            if (chatCompletionProviderHasTopKParameter(model.provider()) == false) {
                throw new ElasticsearchStatusException(
                    Strings.format("The [%s] task parameter is not available for provider [%s]", TOP_K_FIELD, model.provider()),
                    RestStatus.BAD_REQUEST
                );
            }
        }
    }

    @Override
    public void close() throws IOException {
        super.close();
        IOUtils.closeWhileHandlingException(amazonBedrockSender);
    }

    public static class Configuration {
        public static InferenceServiceConfiguration get() {
            return configuration.getOrCompute();
        }

        private static final LazyInitializable<InferenceServiceConfiguration, RuntimeException> configuration = new LazyInitializable<>(
            () -> {
                var configurationMap = new HashMap<String, SettingsConfiguration>();

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
                            Stream.of("amazontitan", "anthropic", "ai21labs", "cohere", "meta", "mistral")
                                .map(v -> new SettingsConfigurationSelectOption.Builder().setLabelAndValue(v).build())
                                .toList()
                        )
                        .build()
                );

                configurationMap.put(
                    MODEL_FIELD,
                    new SettingsConfiguration.Builder().setDisplay(SettingsConfigurationDisplayType.TEXTBOX)
                        .setLabel("Model")
                        .setOrder(4)
                        .setRequired(true)
                        .setSensitive(false)
                        .setTooltip("The base model ID or an ARN to a custom model based on a foundational model.")
                        .setType(SettingsConfigurationFieldType.STRING)
                        .build()
                );

                configurationMap.put(
                    REGION_FIELD,
                    new SettingsConfiguration.Builder().setDisplay(SettingsConfigurationDisplayType.TEXTBOX)
                        .setLabel("Region")
                        .setOrder(5)
                        .setRequired(true)
                        .setSensitive(false)
                        .setTooltip("The region that your model or ARN is deployed in.")
                        .setType(SettingsConfigurationFieldType.STRING)
                        .build()
                );

                configurationMap.putAll(AmazonBedrockSecretSettings.Configuration.get());
                configurationMap.putAll(
                    RateLimitSettings.toSettingsConfigurationWithTooltip(
                        "By default, the amazonbedrock service sets the number of requests allowed per minute to 240."
                    )
                );

                return new InferenceServiceConfiguration.Builder().setProvider(NAME).setTaskTypes(supportedTaskTypes.stream().map(t -> {
                    Map<String, SettingsConfiguration> taskSettingsConfig;
                    switch (t) {
                        case COMPLETION -> taskSettingsConfig = AmazonBedrockChatCompletionModel.Configuration.get();
                        // TEXT_EMBEDDING task type has no task settings
                        default -> taskSettingsConfig = EmptySettingsConfiguration.get();
                    }
                    return new TaskSettingsConfiguration.Builder().setTaskType(t).setConfiguration(taskSettingsConfig).build();
                }).toList()).setConfiguration(configurationMap).build();
            }
        );
    }
}
