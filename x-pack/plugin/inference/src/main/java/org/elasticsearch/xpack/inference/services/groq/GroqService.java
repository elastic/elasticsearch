/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.groq;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkInferenceInput;
import org.elasticsearch.inference.ChunkedInference;
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
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.GenericRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.SenderService;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.groq.action.GroqActionCreator;
import org.elasticsearch.xpack.inference.services.groq.completion.GroqChatCompletionModel;
import org.elasticsearch.xpack.inference.services.groq.request.GroqUnifiedChatCompletionRequest;
import org.elasticsearch.xpack.inference.services.openai.OpenAiServiceFields;
import org.elasticsearch.xpack.inference.services.openai.OpenAiUnifiedChatCompletionResponseHandler;
import org.elasticsearch.xpack.inference.services.openai.response.OpenAiChatCompletionResponseEntity;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;

public class GroqService extends SenderService {
    public static final String NAME = "groq";
    private static final String SERVICE_NAME = "Groq";

    private static final EnumSet<TaskType> SUPPORTED_TASK_TYPES = EnumSet.of(TaskType.CHAT_COMPLETION);
    public static final TransportVersion GROQ_INFERENCE_SERVICE = TransportVersion.fromName("ml_groq_inference_service");
    static final ResponseHandler UNIFIED_CHAT_COMPLETION_HANDLER = new OpenAiUnifiedChatCompletionResponseHandler(
        GroqActionCreator.COMPLETION_REQUEST_TYPE,
        OpenAiChatCompletionResponseEntity::fromResponse
    );

    public GroqService(
        HttpRequestSender.Factory factory,
        ServiceComponents serviceComponents,
        InferenceServiceExtension.InferenceServiceFactoryContext context
    ) {
        this(factory, serviceComponents, context.clusterService());
    }

    public GroqService(HttpRequestSender.Factory factory, ServiceComponents serviceComponents, ClusterService clusterService) {
        super(factory, serviceComponents, clusterService);
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public EnumSet<TaskType> supportedTaskTypes() {
        return SUPPORTED_TASK_TYPES;
    }

    @Override
    public Set<TaskType> supportedStreamingTasks() {
        return SUPPORTED_TASK_TYPES;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return GROQ_INFERENCE_SERVICE;
    }

    @Override
    public void parseRequestConfig(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> config,
        ActionListener<Model> parsedModelListener
    ) {
        try {
            Map<String, Object> serviceSettingsMap = ServiceUtils.removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
            Map<String, Object> taskSettingsMap = ServiceUtils.removeFromMapOrDefaultEmpty(config, ModelConfigurations.TASK_SETTINGS);

            GroqChatCompletionModel model = createModel(
                inferenceEntityId,
                taskType,
                serviceSettingsMap,
                taskSettingsMap,
                serviceSettingsMap,
                ConfigurationParseContext.REQUEST
            );

            ServiceUtils.throwIfNotEmptyMap(config, NAME);
            ServiceUtils.throwIfNotEmptyMap(serviceSettingsMap, NAME);
            ServiceUtils.throwIfNotEmptyMap(taskSettingsMap, NAME);

            parsedModelListener.onResponse(model);
        } catch (Exception e) {
            parsedModelListener.onFailure(e);
        }
    }

    @Override
    public GroqChatCompletionModel parsePersistedConfigWithSecrets(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> config,
        Map<String, Object> secrets
    ) {
        Map<String, Object> serviceSettingsMap = ServiceUtils.removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
        Map<String, Object> taskSettingsMap = ServiceUtils.removeFromMapOrDefaultEmpty(config, ModelConfigurations.TASK_SETTINGS);
        Map<String, Object> secretSettingsMap = ServiceUtils.removeFromMapOrDefaultEmpty(secrets, ModelSecrets.SECRET_SETTINGS);

        return createModel(
            inferenceEntityId,
            taskType,
            serviceSettingsMap,
            taskSettingsMap,
            secretSettingsMap,
            ConfigurationParseContext.PERSISTENT
        );
    }

    @Override
    public GroqChatCompletionModel parsePersistedConfig(String inferenceEntityId, TaskType taskType, Map<String, Object> config) {
        Map<String, Object> serviceSettingsMap = ServiceUtils.removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
        Map<String, Object> taskSettingsMap = ServiceUtils.removeFromMapOrDefaultEmpty(config, ModelConfigurations.TASK_SETTINGS);

        return createModel(inferenceEntityId, taskType, serviceSettingsMap, taskSettingsMap, null, ConfigurationParseContext.PERSISTENT);
    }

    private static GroqChatCompletionModel createModel(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        Map<String, Object> secretSettings,
        ConfigurationParseContext context
    ) {
        if (SUPPORTED_TASK_TYPES.contains(taskType) == false) {
            throw ServiceUtils.createInvalidTaskTypeException(inferenceEntityId, NAME, taskType, context);
        }

        return new GroqChatCompletionModel(inferenceEntityId, taskType, NAME, serviceSettings, taskSettings, secretSettings, context);
    }

    @Override
    public void doInfer(
        Model model,
        InferenceInputs inputs,
        Map<String, Object> taskSettings,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        if ((model instanceof GroqChatCompletionModel) == false) {
            listener.onFailure(ServiceUtils.createInvalidModelException(model));
            return;
        }

        GroqChatCompletionModel groqModel = (GroqChatCompletionModel) model;
        var actionCreator = new GroqActionCreator(getSender(), getServiceComponents());
        var action = groqModel.accept(actionCreator, taskSettings);
        action.execute(inputs, timeout, listener);
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
        if ((model instanceof GroqChatCompletionModel) == false) {
            listener.onFailure(ServiceUtils.createInvalidModelException(model));
            return;
        }

        GroqChatCompletionModel groqModel = (GroqChatCompletionModel) model;
        var overriddenModel = GroqChatCompletionModel.of(groqModel, inputs.getRequest());
        var manager = new GenericRequestManager<>(
            getServiceComponents().threadPool(),
            overriddenModel,
            UNIFIED_CHAT_COMPLETION_HANDLER,
            unifiedChatInput -> new GroqUnifiedChatCompletionRequest(unifiedChatInput, overriddenModel),
            UnifiedChatInput.class
        );

        var errorMessage = constructFailedToSendRequestMessage(GroqActionCreator.COMPLETION_ERROR_PREFIX);
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
        listener.onFailure(new ElasticsearchStatusException("Groq does not support chunked inference", RestStatus.BAD_REQUEST));
    }

    @Override
    public InferenceServiceConfiguration getConfiguration() {
        return Configuration.get();
    }

    public static class Configuration {
        public static InferenceServiceConfiguration get() {
            return CONFIGURATION.getOrCompute();
        }

        private static final LazyInitializable<InferenceServiceConfiguration, RuntimeException> CONFIGURATION = new LazyInitializable<>(
            () -> {
                var configurationMap = new HashMap<String, SettingsConfiguration>();

                configurationMap.put(
                    ServiceFields.URL,
                    new SettingsConfiguration.Builder(SUPPORTED_TASK_TYPES).setDefaultValue(
                        "https://api.groq.com/openai/v1/chat/completions"
                    )
                        .setDescription("The absolute URL of the Groq chat completions endpoint.")
                        .setLabel("URL")
                        .setRequired(false)
                        .setSensitive(false)
                        .setUpdatable(false)
                        .setType(SettingsConfigurationFieldType.STRING)
                        .build()
                );

                configurationMap.put(
                    ServiceFields.MODEL_ID,
                    new SettingsConfiguration.Builder(SUPPORTED_TASK_TYPES).setDescription("The Groq model ID to use for this endpoint.")
                        .setLabel("Model ID")
                        .setRequired(true)
                        .setSensitive(false)
                        .setUpdatable(false)
                        .setType(SettingsConfigurationFieldType.STRING)
                        .build()
                );

                configurationMap.put(
                    OpenAiServiceFields.ORGANIZATION,
                    new SettingsConfiguration.Builder(SUPPORTED_TASK_TYPES).setDescription(
                        "Optional Groq organization identifier to scope API usage."
                    )
                        .setLabel("Organization ID")
                        .setRequired(false)
                        .setSensitive(false)
                        .setUpdatable(false)
                        .setType(SettingsConfigurationFieldType.STRING)
                        .build()
                );

                configurationMap.put(
                    OpenAiServiceFields.HEADERS,
                    new SettingsConfiguration.Builder(SUPPORTED_TASK_TYPES).setDescription(
                        "Additional headers to include with Groq requests."
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
                        "The Groq API key. Generate keys from https://console.groq.com/keys.",
                        SUPPORTED_TASK_TYPES
                    )
                );

                configurationMap.putAll(
                    RateLimitSettings.toSettingsConfigurationWithDescription(
                        "Default number of requests per minute allowed for the Groq API.",
                        SUPPORTED_TASK_TYPES
                    )
                );

                return new InferenceServiceConfiguration.Builder().setService(NAME)
                    .setName(SERVICE_NAME)
                    .setTaskTypes(SUPPORTED_TASK_TYPES)
                    .setConfigurations(configurationMap)
                    .build();
            }
        );
    }
}
