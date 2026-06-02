/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkInferenceInput;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.EmbeddingRequest;
import org.elasticsearch.inference.InferenceServiceConfiguration;
import org.elasticsearch.inference.InferenceServiceExtension;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.SettingsConfiguration;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.configuration.SettingsConfigurationFieldType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.chunking.EmbeddingRequestChunker;
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.external.http.sender.GenericRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.ModelCreator;
import org.elasticsearch.xpack.inference.services.SenderService;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.openai.action.OpenAiActionCreator;
import org.elasticsearch.xpack.inference.services.openai.completion.OpenAiChatCompletionModel;
import org.elasticsearch.xpack.inference.services.openai.completion.OpenAiChatCompletionModelCreator;
import org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsModelCreator;
import org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.openai.request.OpenAiUnifiedChatCompletionRequest;
import org.elasticsearch.xpack.inference.services.openai.response.OpenAiChatCompletionResponseEntity;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;
import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createInvalidModelException;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createUnsupportedTaskTypeStatusException;
import static org.elasticsearch.xpack.inference.services.openai.OpenAiServiceFields.EMBEDDING_MAX_BATCH_SIZE;
import static org.elasticsearch.xpack.inference.services.openai.OpenAiServiceFields.HEADERS;
import static org.elasticsearch.xpack.inference.services.openai.OpenAiServiceFields.ORGANIZATION;
import static org.elasticsearch.xpack.inference.services.openai.action.OpenAiActionCreator.COMPLETION_ERROR_PREFIX;

public class OpenAiService extends SenderService<OpenAiModel> {
    public static final String NAME = "openai";

    private static final String SERVICE_NAME = "OpenAI";
    // The task types exposed via the _inference/_services API
    private static final EnumSet<TaskType> SUPPORTED_TASK_TYPES_FOR_SERVICES_API = EnumSet.of(
        TaskType.TEXT_EMBEDDING,
        TaskType.COMPLETION,
        TaskType.CHAT_COMPLETION,
        TaskType.EMBEDDING
    );
    /**
     * The task types that the {@link InferenceAction.Request} can accept.
     */
    private static final EnumSet<TaskType> SUPPORTED_INFERENCE_ACTION_TASK_TYPES = EnumSet.of(TaskType.TEXT_EMBEDDING, TaskType.COMPLETION);
    private static final ResponseHandler UNIFIED_CHAT_COMPLETION_HANDLER = new OpenAiUnifiedChatCompletionResponseHandler(
        "openai completion",
        OpenAiChatCompletionResponseEntity::fromResponse
    );
    private static final OpenAiChatCompletionModelCreator COMPLETION_MODEL_CREATOR = new OpenAiChatCompletionModelCreator();
    public static final OpenAiEmbeddingsModelCreator EMBEDDINGS_MODEL_CREATOR = new OpenAiEmbeddingsModelCreator();

    private static Map<TaskType, ModelCreator<? extends OpenAiModel>> initModelCreators() {
        return Map.of(
            TaskType.TEXT_EMBEDDING,
            EMBEDDINGS_MODEL_CREATOR,
            TaskType.COMPLETION,
            COMPLETION_MODEL_CREATOR,
            TaskType.CHAT_COMPLETION,
            COMPLETION_MODEL_CREATOR,
            TaskType.EMBEDDING,
            EMBEDDINGS_MODEL_CREATOR
        );
    }

    public OpenAiService(
        HttpRequestSender.Factory factory,
        ServiceComponents serviceComponents,
        InferenceServiceExtension.InferenceServiceFactoryContext context
    ) {
        this(factory, serviceComponents, context.clusterService());
    }

    public OpenAiService(HttpRequestSender.Factory factory, ServiceComponents serviceComponents, ClusterService clusterService) {
        super(factory, serviceComponents, clusterService, initModelCreators());
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    protected void migrateBetweenTaskAndServiceSettings(Map<String, Object> serviceSettings, Map<String, Object> taskSettings) {
        moveModelFromTaskToServiceSettings(taskSettings, serviceSettings);
    }

    @Override
    public InferenceServiceConfiguration getConfiguration() {
        return Configuration.get();
    }

    @Override
    public EnumSet<TaskType> supportedTaskTypes() {
        return SUPPORTED_TASK_TYPES_FOR_SERVICES_API;
    }

    @Override
    public void doInfer(
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

        if (model instanceof OpenAiModel == false) {
            listener.onFailure(createInvalidModelException(model));
            return;
        }

        OpenAiModel openAiModel = (OpenAiModel) model;
        var actionCreator = new OpenAiActionCreator(getSender(), getServiceComponents());

        var action = openAiModel.accept(actionCreator, taskSettings);
        action.execute(inputs, timeout, listener);
    }

    @Override
    protected void validateInputType(InputType inputType, Model model, ValidationException validationException) {
        ServiceUtils.validateInputTypeIsUnspecifiedOrInternal(inputType, validationException);
    }

    @Override
    public void doUnifiedCompletionInfer(
        Model model,
        UnifiedChatInput inputs,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        if (model instanceof OpenAiChatCompletionModel == false) {
            listener.onFailure(createInvalidModelException(model));
            return;
        }

        OpenAiChatCompletionModel openAiModel = (OpenAiChatCompletionModel) model;

        var overriddenModel = OpenAiChatCompletionModel.of(openAiModel, inputs.getRequest());

        var manager = new GenericRequestManager<>(
            getServiceComponents().threadPool(),
            overriddenModel,
            UNIFIED_CHAT_COMPLETION_HANDLER,
            (unifiedChatInput) -> new OpenAiUnifiedChatCompletionRequest(unifiedChatInput, overriddenModel),
            UnifiedChatInput.class
        );

        var errorMessage = constructFailedToSendRequestMessage(COMPLETION_ERROR_PREFIX);
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
        if (model instanceof OpenAiModel == false) {
            listener.onFailure(createInvalidModelException(model));
            return;
        }

        OpenAiModel openAiModel = (OpenAiModel) model;
        var actionCreator = new OpenAiActionCreator(getSender(), getServiceComponents());

        List<EmbeddingRequestChunker.BatchRequestAndListener> batchedRequests = new EmbeddingRequestChunker<>(
            inputs,
            EMBEDDING_MAX_BATCH_SIZE,
            openAiModel.getConfigurations().getChunkingSettings()
        ).batchRequestsWithListeners(listener);

        for (var request : batchedRequests) {
            var action = openAiModel.accept(actionCreator, taskSettings);
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
        if (model instanceof OpenAiModel == false) {
            listener.onFailure(createInvalidModelException(model));
            return;
        }

        OpenAiModel openAiModel = (OpenAiModel) model;
        var actionCreator = new OpenAiActionCreator(getSender(), getServiceComponents());

        var action = openAiModel.accept(actionCreator, request.taskSettings());
        action.execute(new EmbeddingsInput(request::inputs, request.inputType()), timeout, listener);
    }

    @Override
    public Model updateModelWithEmbeddingDetails(Model model, int embeddingSize) {
        if (model instanceof OpenAiEmbeddingsModel embeddingsModel) {
            var serviceSettings = embeddingsModel.getServiceSettings();
            var similarityFromModel = serviceSettings.similarity();
            var similarityToUse = similarityFromModel == null ? SimilarityMeasure.DOT_PRODUCT : similarityFromModel;

            var updatedServiceSettings = new OpenAiEmbeddingsServiceSettings(
                serviceSettings.modelId(),
                serviceSettings.uri(),
                serviceSettings.organizationId(),
                similarityToUse,
                embeddingSize,
                serviceSettings.maxInputTokens(),
                serviceSettings.dimensionsSetByUser(),
                serviceSettings.rateLimitSettings()
            );

            return new OpenAiEmbeddingsModel(embeddingsModel, updatedServiceSettings);
        } else {
            throw ServiceUtils.invalidModelTypeForUpdateModelWithEmbeddingDetails(model.getClass());
        }
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.minimumCompatible();
    }

    @Override
    public Set<TaskType> supportedStreamingTasks() {
        return EnumSet.of(TaskType.COMPLETION, TaskType.CHAT_COMPLETION);
    }

    /**
     * Model was originally defined in task settings, but it should
     * have been part of the service settings.
     *
     * If model or model_id are in the task settings map move
     * them to service settings ready for parsing
     *
     * @param taskSettings Task settings map
     * @param serviceSettings Service settings map
     */
    static void moveModelFromTaskToServiceSettings(Map<String, Object> taskSettings, Map<String, Object> serviceSettings) {
        if (serviceSettings.containsKey(MODEL_ID)) {
            return;
        }

        final String OLD_MODEL_ID_FIELD = "model";
        var oldModelId = taskSettings.remove(OLD_MODEL_ID_FIELD);
        if (oldModelId != null) {
            serviceSettings.put(MODEL_ID, oldModelId);
        } else {
            var modelId = taskSettings.remove(MODEL_ID);
            serviceSettings.put(MODEL_ID, modelId);
        }
    }

    public static class Configuration {
        public static InferenceServiceConfiguration get() {
            return CONFIGURATION.getOrCompute();
        }

        private static final LazyInitializable<InferenceServiceConfiguration, RuntimeException> CONFIGURATION = new LazyInitializable<>(
            () -> {
                var configurationMap = new HashMap<String, SettingsConfiguration>();

                configurationMap.put(
                    URL,
                    new SettingsConfiguration.Builder(SUPPORTED_TASK_TYPES_FOR_SERVICES_API).setDescription(
                        "The absolute URL of the external service to send requests to."
                    )
                        .setLabel("URL")
                        .setRequired(false)
                        .setSensitive(false)
                        .setUpdatable(false)
                        .setType(SettingsConfigurationFieldType.STRING)
                        .build()
                );

                configurationMap.put(
                    MODEL_ID,
                    new SettingsConfiguration.Builder(SUPPORTED_TASK_TYPES_FOR_SERVICES_API).setDescription(
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
                    ORGANIZATION,
                    new SettingsConfiguration.Builder(SUPPORTED_TASK_TYPES_FOR_SERVICES_API).setDescription(
                        "The unique identifier of your organization."
                    )
                        .setLabel("Organization ID")
                        .setRequired(false)
                        .setSensitive(false)
                        .setUpdatable(false)
                        .setType(SettingsConfigurationFieldType.STRING)
                        .build()
                );

                configurationMap.put(
                    DIMENSIONS,
                    new SettingsConfiguration.Builder(EnumSet.of(TaskType.TEXT_EMBEDDING, TaskType.EMBEDDING)).setDescription(
                        "The number of dimensions the resulting embeddings should have. For more information refer to "
                            + "https://platform.openai.com/docs/api-reference/embeddings/create#embeddings-create-dimensions."
                    )
                        .setLabel("Dimensions")
                        .setRequired(false)
                        .setSensitive(false)
                        .setUpdatable(false)
                        .setType(SettingsConfigurationFieldType.INTEGER)
                        .build()
                );

                configurationMap.put(
                    HEADERS,
                    new SettingsConfiguration.Builder(SUPPORTED_TASK_TYPES_FOR_SERVICES_API).setDescription(
                        "Custom headers to include in the requests to OpenAI."
                    )
                        .setLabel("Custom Headers")
                        .setRequired(false)
                        .setSensitive(false)
                        .setUpdatable(true)
                        .setType(SettingsConfigurationFieldType.MAP)
                        .build()
                );

                configurationMap.putAll(
                    DefaultSecretSettings.toSettingsConfigurationWithDescription(
                        "The OpenAI API authentication key. For more details about generating OpenAI API keys, "
                            + "refer to the https://platform.openai.com/account/api-keys.",
                        SUPPORTED_TASK_TYPES_FOR_SERVICES_API
                    )
                );
                configurationMap.putAll(
                    RateLimitSettings.toSettingsConfigurationWithDescription(
                        "Default number of requests allowed per minute. For text_embedding and embedding it is 3000. "
                            + "For completion and chat_completion it is 500.",
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
