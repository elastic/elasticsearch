/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.groq;

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
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.GenericRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.SenderService;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.groq.action.GroqActionCreator;
import org.elasticsearch.xpack.inference.services.groq.completion.GroqChatCompletionModel;
import org.elasticsearch.xpack.inference.services.groq.completion.GroqChatCompletionResponseHandler;
import org.elasticsearch.xpack.inference.services.groq.request.completion.GroqChatCompletionRequest;
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
import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createInvalidModelException;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createInvalidTaskTypeException;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrDefaultEmpty;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrThrowIfNull;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.throwIfNotEmptyMap;

/**
 * GroqService is an inference service for Groq models, supporting chat completion tasks.
 * It extends {@link SenderService} to handle HTTP requests and responses for Groq models.
 */
public class GroqService extends SenderService {
    public static final String NAME = "groq";
    private static final String SERVICE_NAME = "Groq";

    private static final EnumSet<TaskType> SUPPORTED_TASK_TYPES = EnumSet.of(TaskType.COMPLETION, TaskType.CHAT_COMPLETION);
    private static final ResponseHandler UNIFIED_CHAT_COMPLETION_HANDLER = new GroqChatCompletionResponseHandler(
        "Groq chat completion",
        OpenAiChatCompletionResponseEntity::fromResponse
    );

    /**
     * Constructor for creating a {@link GroqService} with specified HTTP request sender factory and service components.
     *
     * @param factory the factory to create HTTP request senders
     * @param serviceComponents the components required for the inference service
     * @param context the context for the inference service factory
     */
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
    protected void doInfer(
        Model model,
        InferenceInputs inputs,
        Map<String, Object> taskSettings,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        var actionCreator = new GroqActionCreator(getSender(), getServiceComponents());
        if (model instanceof GroqModel groqModel) {
            groqModel.accept(actionCreator).execute(inputs, timeout, listener);
        } else {
            listener.onFailure(createInvalidModelException(model));
        }
    }

    /**
     * Creates a {@link GroqModel} based on the provided parameters.
     *
     * @param inferenceId the unique identifier for the inference entity
     * @param taskType the type of task this model is designed for
     * @param serviceSettings the settings for the inference service
     * @param secretSettings the secret settings for the model
     * @param context the context for parsing configuration settings
     * @return a new instance of {@link GroqModel} based on the provided parameters
     */
    protected GroqModel createModel(
        String inferenceId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        Map<String, Object> secretSettings,
        ConfigurationParseContext context
    ) {
        return switch (taskType) {
            case CHAT_COMPLETION, COMPLETION -> new GroqChatCompletionModel(
                inferenceId,
                taskType,
                NAME,
                serviceSettings,
                secretSettings,
                context
            );
            default -> throw createInvalidTaskTypeException(inferenceId, NAME, taskType, context);
        };
    }

    @Override
    protected void doUnifiedCompletionInfer(
        Model model,
        UnifiedChatInput inputs,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        if (model instanceof GroqChatCompletionModel == false) {
            listener.onFailure(createInvalidModelException(model));
            return;
        }

        var groqChatCompletionModel = (GroqChatCompletionModel) model;
        var overriddenModel = GroqChatCompletionModel.of(groqChatCompletionModel, inputs.getRequest().model());
        var manager = new GenericRequestManager<>(
            getServiceComponents().threadPool(),
            overriddenModel,
            UNIFIED_CHAT_COMPLETION_HANDLER,
            unifiedChatInput -> new GroqChatCompletionRequest(unifiedChatInput, overriddenModel),
            UnifiedChatInput.class
        );
        var errorMessage = GroqActionCreator.buildErrorMessage(CHAT_COMPLETION, model.getInferenceEntityId());
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
        // Should never be called
        throw new UnsupportedOperationException("Groq service does not support chunked inference");
    }

    @Override
    protected boolean supportsChunkedInfer() {
        return false;
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
        String inferenceId,
        TaskType taskType,
        Map<String, Object> config,
        ActionListener<Model> parsedModelListener
    ) {
        try {
            Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
            Map<String, Object> taskSettingsMap = removeFromMapOrDefaultEmpty(config, ModelConfigurations.TASK_SETTINGS);

            GroqModel model = createModel(inferenceId, taskType, serviceSettingsMap, serviceSettingsMap, ConfigurationParseContext.REQUEST);

            throwIfNotEmptyMap(config, NAME);
            throwIfNotEmptyMap(serviceSettingsMap, NAME);
            throwIfNotEmptyMap(taskSettingsMap, NAME);

            parsedModelListener.onResponse(model);
        } catch (Exception e) {
            parsedModelListener.onFailure(e);
        }
    }

    private GroqModel createModelFromPersistent(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        Map<String, Object> secretSettings
    ) {
        return createModel(inferenceEntityId, taskType, serviceSettings, secretSettings, ConfigurationParseContext.PERSISTENT);
    }

    private GroqModel parsePersistedConfigInternal(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> config,
        Map<String, Object> secrets
    ) {
        Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
        Map<String, Object> secretSettingsMap = null;
        if (secrets != null) {
            secretSettingsMap = removeFromMapOrDefaultEmpty(secrets, ModelSecrets.SECRET_SETTINGS);
        }

        return createModelFromPersistent(inferenceEntityId, taskType, serviceSettingsMap, secretSettingsMap);
    }

    @Override
    public GroqModel parsePersistedConfigWithSecrets(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> config,
        Map<String, Object> secrets
    ) {
        return parsePersistedConfigInternal(inferenceEntityId, taskType, config, secrets);
    }

    @Override
    public GroqModel parsePersistedConfig(String inferenceEntityId, TaskType taskType, Map<String, Object> config) {
        return parsePersistedConfigInternal(inferenceEntityId, taskType, config, null);
    }

    @Override
    protected void validateInputType(InputType inputType, Model model, ValidationException validationException) {
        ServiceUtils.validateInputTypeIsUnspecifiedOrInternal(inputType, validationException);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return GroqUtils.ML_INFERENCE_GROQ_ADDED;
    }

    /**
     * Configuration class for the Groq inference service.
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
                    MODEL_ID,
                    new SettingsConfiguration.Builder(SUPPORTED_TASK_TYPES).setDescription("""
                        The name of the model to use for the inference task. Refer to the \
                        Groq models documentation for the list of available models.""")
                        .setLabel("Model ID")
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
