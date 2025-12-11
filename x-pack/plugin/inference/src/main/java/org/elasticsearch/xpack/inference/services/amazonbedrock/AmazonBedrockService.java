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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.core.IOUtils;
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
import org.elasticsearch.inference.SettingsConfiguration;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.configuration.SettingsConfigurationFieldType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsBuilder;
import org.elasticsearch.xpack.core.inference.chunking.EmbeddingRequestChunker;
import org.elasticsearch.xpack.inference.common.amazon.AwsSecretSettings;
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.SenderService;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.amazonbedrock.action.AmazonBedrockActionCreator;
import org.elasticsearch.xpack.inference.services.amazonbedrock.client.AmazonBedrockRequestSender;
import org.elasticsearch.xpack.inference.services.amazonbedrock.completion.AmazonBedrockChatCompletionModel;
import org.elasticsearch.xpack.inference.services.amazonbedrock.embeddings.AmazonBedrockEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.amazonbedrock.embeddings.AmazonBedrockEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;
import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createInvalidModelException;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createInvalidTaskTypeException;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createUnsupportedTaskTypeStatusException;
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

/**
 * TODO we should remove AmazonBedrockService's dependency on SenderService. Bedrock leverages its own SDK with handles sending requests
 * and already implements rate limiting.
 *
 * https://github.com/elastic/ml-team/issues/1706
 */
public class AmazonBedrockService extends SenderService {
    public static final String NAME = "amazonbedrock";
    private static final String SERVICE_NAME = "Amazon Bedrock";
    public static final String CHAT_COMPLETION_ERROR_PREFIX = "Amazon Bedrock chat completion";

    private final Sender amazonBedrockSender;

    // The task types exposed via the _inference/_services API
    private static final EnumSet<TaskType> SUPPORTED_TASK_TYPES_FOR_SERVICES_API = EnumSet.of(
        TaskType.TEXT_EMBEDDING,
        TaskType.COMPLETION,
        TaskType.CHAT_COMPLETION
    );
    /**
     * The task types that the {@link InferenceAction.Request} can accept.
     */
    private static final EnumSet<TaskType> SUPPORTED_INFERENCE_ACTION_TASK_TYPES = EnumSet.of(TaskType.TEXT_EMBEDDING, TaskType.COMPLETION);

    private static final EnumSet<InputType> VALID_INPUT_TYPE_VALUES = EnumSet.of(
        InputType.INGEST,
        InputType.SEARCH,
        InputType.CLASSIFICATION,
        InputType.CLUSTERING,
        InputType.INTERNAL_INGEST,
        InputType.INTERNAL_SEARCH,
        InputType.UNSPECIFIED
    );

    public AmazonBedrockService(
        HttpRequestSender.Factory httpSenderFactory,
        AmazonBedrockRequestSender.Factory amazonBedrockFactory,
        ServiceComponents serviceComponents,
        InferenceServiceExtension.InferenceServiceFactoryContext context
    ) {
        this(httpSenderFactory, amazonBedrockFactory, serviceComponents, context.clusterService());
    }

    public AmazonBedrockService(
        HttpRequestSender.Factory httpSenderFactory,
        AmazonBedrockRequestSender.Factory amazonBedrockFactory,
        ServiceComponents serviceComponents,
        ClusterService clusterService
    ) {
        super(httpSenderFactory, serviceComponents, clusterService);
        this.amazonBedrockSender = amazonBedrockFactory.createSender();
    }

    @Override
    protected void doUnifiedCompletionInfer(
        Model model,
        UnifiedChatInput inputs,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        if (model instanceof AmazonBedrockChatCompletionModel == false) {
            listener.onFailure(createInvalidModelException(model));
            return;
        }

        AmazonBedrockChatCompletionModel amazonBedrockChatCompletionModel = (AmazonBedrockChatCompletionModel) model;

        var overriddenModel = AmazonBedrockChatCompletionModel.of(amazonBedrockChatCompletionModel, inputs.getRequest());

        var manager = new AmazonBedrockChatCompletionRequestManager(overriddenModel, this.getServiceComponents().threadPool(), timeout);
        var errorMessage = constructFailedToSendRequestMessage(AmazonBedrockService.CHAT_COMPLETION_ERROR_PREFIX);
        var action = new SenderExecutableAction(amazonBedrockSender, manager, errorMessage);
        action.execute(inputs, timeout, listener);
    }

    @Override
    protected void doInfer(
        Model model,
        InferenceInputs inputs,
        Map<String, Object> taskSettings,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        if (SUPPORTED_INFERENCE_ACTION_TASK_TYPES.contains(model.getTaskType()) == false) {
            listener.onFailure(createUnsupportedTaskTypeStatusException(model, SUPPORTED_INFERENCE_ACTION_TASK_TYPES));
            return;
        }

        if (model instanceof AmazonBedrockModel == false) {
            listener.onFailure(createInvalidModelException(model));
            return;
        }

        AmazonBedrockModel baseAmazonBedrockModel = (AmazonBedrockModel) model;
        var actionCreator = new AmazonBedrockActionCreator(amazonBedrockSender, this.getServiceComponents(), timeout);
        var action = baseAmazonBedrockModel.accept(actionCreator, taskSettings);
        action.execute(inputs, timeout, listener);
    }

    @Override
    protected void validateInputType(InputType inputType, Model model, ValidationException validationException) {
        if (model instanceof AmazonBedrockModel) {
            ServiceUtils.validateInputTypeAgainstAllowlist(inputType, VALID_INPUT_TYPE_VALUES, SERVICE_NAME, validationException);
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
        var actionCreator = new AmazonBedrockActionCreator(amazonBedrockSender, this.getServiceComponents(), timeout);
        if (model instanceof AmazonBedrockModel baseAmazonBedrockModel) {
            var maxBatchSize = getEmbeddingsMaxBatchSize(baseAmazonBedrockModel.provider());

            List<EmbeddingRequestChunker.BatchRequestAndListener> batchedRequests = new EmbeddingRequestChunker<>(
                inputs,
                maxBatchSize,
                baseAmazonBedrockModel.getConfigurations().getChunkingSettings()
            ).batchRequestsWithListeners(listener);

            for (var request : batchedRequests) {
                var action = baseAmazonBedrockModel.accept(actionCreator, taskSettings);
                action.execute(new EmbeddingsInput(request.batch().inputs(), inputType), timeout, request.listener());
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
            ConfigurationParseContext.PERSISTENT
        );
    }

    @Override
    public InferenceServiceConfiguration getConfiguration() {
        return Configuration.get();
    }

    @Override
    public EnumSet<TaskType> supportedTaskTypes() {
        return SUPPORTED_TASK_TYPES_FOR_SERVICES_API;
    }

    private static AmazonBedrockModel createModel(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        ChunkingSettings chunkingSettings,
        @Nullable Map<String, Object> secretSettings,
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
                checkProviderForTask(taskType, model.provider());
                checkTaskSettingsForTextEmbeddingModel(model);
                return model;
            }
            case COMPLETION, CHAT_COMPLETION -> {
                var model = new AmazonBedrockChatCompletionModel(
                    inferenceEntityId,
                    taskType,
                    NAME,
                    serviceSettings,
                    taskSettings,
                    secretSettings,
                    context
                );
                checkProviderForTask(taskType, model.provider());
                checkChatCompletionProviderForTopKParameter(model);
                return model;
            }
            default -> throw createInvalidTaskTypeException(inferenceEntityId, NAME, taskType, context);
        }
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_15_0;
    }

    @Override
    public Set<TaskType> supportedStreamingTasks() {
        return EnumSet.of(TaskType.COMPLETION, TaskType.CHAT_COMPLETION);
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

    private static void checkTaskSettingsForTextEmbeddingModel(AmazonBedrockEmbeddingsModel model) {
        if (model.provider() != AmazonBedrockProvider.COHERE && model.getTaskSettings().truncation() != null) {
            throw new ElasticsearchStatusException(
                "The [{}] task type for provider [{}] does not allow [truncate] field",
                RestStatus.BAD_REQUEST,
                TaskType.TEXT_EMBEDDING,
                model.provider()
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
                    new SettingsConfiguration.Builder(SUPPORTED_TASK_TYPES_FOR_SERVICES_API).setDescription(
                        "The model provider for your deployment."
                    )
                        .setLabel("Provider")
                        .setRequired(true)
                        .setSensitive(false)
                        .setUpdatable(false)
                        .setType(SettingsConfigurationFieldType.STRING)
                        .build()
                );

                configurationMap.put(
                    MODEL_FIELD,
                    new SettingsConfiguration.Builder(SUPPORTED_TASK_TYPES_FOR_SERVICES_API).setDescription(
                        "The base model ID or an ARN to a custom model based on a foundational model."
                    )
                        .setLabel("Model")
                        .setRequired(true)
                        .setSensitive(false)
                        .setUpdatable(false)
                        .setType(SettingsConfigurationFieldType.STRING)
                        .build()
                );

                configurationMap.put(
                    REGION_FIELD,
                    new SettingsConfiguration.Builder(SUPPORTED_TASK_TYPES_FOR_SERVICES_API).setDescription(
                        "The region that your model or ARN is deployed in."
                    )
                        .setLabel("Region")
                        .setRequired(true)
                        .setSensitive(false)
                        .setUpdatable(false)
                        .setType(SettingsConfigurationFieldType.STRING)
                        .build()
                );

                configurationMap.put(
                    DIMENSIONS,
                    new SettingsConfiguration.Builder(EnumSet.of(TaskType.TEXT_EMBEDDING)).setDescription(
                        "The number of dimensions the resulting embeddings should have. For more information refer to "
                            + "https://docs.aws.amazon.com/bedrock/latest/userguide/model-parameters-titan-embed-text.html."
                    )
                        .setLabel("Dimensions")
                        .setRequired(false)
                        .setSensitive(false)
                        .setUpdatable(false)
                        .setType(SettingsConfigurationFieldType.INTEGER)
                        .build()
                );

                configurationMap.putAll(AwsSecretSettings.Configuration.get());
                configurationMap.putAll(
                    RateLimitSettings.toSettingsConfigurationWithDescription(
                        "By default, the amazonbedrock service sets the number of requests allowed per minute to 240.",
                        SUPPORTED_TASK_TYPES_FOR_SERVICES_API
                    )
                );

                return new InferenceServiceConfiguration.Builder().setService(NAME)
                    .setName(SERVICE_NAME)
                    .setTaskTypes(SUPPORTED_TASK_TYPES_FOR_SERVICES_API)
                    .setConfigurations(configurationMap)
                    .build();
            }
        );
    }
}
