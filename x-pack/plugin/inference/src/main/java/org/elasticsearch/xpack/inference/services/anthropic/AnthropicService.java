/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.anthropic;

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
import org.elasticsearch.inference.SettingsConfiguration;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.configuration.SettingsConfigurationFieldType;
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.GenericRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.ModelCreator;
import org.elasticsearch.xpack.inference.services.SenderService;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.anthropic.action.AnthropicActionCreator;
import org.elasticsearch.xpack.inference.services.anthropic.completion.AnthropicChatCompletionModel;
import org.elasticsearch.xpack.inference.services.anthropic.completion.AnthropicChatCompletionModelCreator;
import org.elasticsearch.xpack.inference.services.anthropic.request.AnthropicUnifiedChatCompletionRequest;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createInvalidModelException;

public class AnthropicService extends SenderService<AnthropicModel> {
    public static final String NAME = "anthropic";
    private static final String SERVICE_NAME = "Anthropic";

    private static final EnumSet<TaskType> SUPPORTED_TASK_TYPES = EnumSet.of(TaskType.COMPLETION, TaskType.CHAT_COMPLETION);
    private static final AnthropicChatCompletionModelCreator COMPLETION_MODEL_CREATOR = new AnthropicChatCompletionModelCreator();
    private static final Map<TaskType, ModelCreator<? extends AnthropicModel>> MODEL_CREATORS = Map.of(
        TaskType.COMPLETION,
        COMPLETION_MODEL_CREATOR,
        TaskType.CHAT_COMPLETION,
        COMPLETION_MODEL_CREATOR
    );

    private static final String CHAT_COMPLETION_ERROR_PREFIX = "anthropic chat completions";
    private static final ResponseHandler UNIFIED_CHAT_COMPLETION_HANDLER = new AnthropicChatCompletionResponseHandler(
        CHAT_COMPLETION_ERROR_PREFIX
    );

    public AnthropicService(
        HttpRequestSender.Factory factory,
        ServiceComponents serviceComponents,
        InferenceServiceExtension.InferenceServiceFactoryContext context
    ) {
        this(factory, serviceComponents, context.clusterService());
    }

    public AnthropicService(HttpRequestSender.Factory factory, ServiceComponents serviceComponents, ClusterService clusterService) {
        super(factory, serviceComponents, clusterService, MODEL_CREATORS);
    }

    @Override
    public String name() {
        return NAME;
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
    protected void doUnifiedCompletionInfer(
        Model model,
        UnifiedChatInput inputs,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        if (model instanceof AnthropicChatCompletionModel == false) {
            listener.onFailure(createInvalidModelException(model));
            return;
        }

        var anthropicModel = (AnthropicChatCompletionModel) model;
        var overriddenModel = AnthropicChatCompletionModel.of(anthropicModel, inputs.getRequest());

        var requestManager = new GenericRequestManager<>(
            getServiceComponents().threadPool(),
            overriddenModel,
            UNIFIED_CHAT_COMPLETION_HANDLER,
            (unifiedChatInput) -> new AnthropicUnifiedChatCompletionRequest(unifiedChatInput, overriddenModel),
            UnifiedChatInput.class
        );

        var errorMessage = constructFailedToSendRequestMessage(CHAT_COMPLETION_ERROR_PREFIX);
        var action = new SenderExecutableAction(getSender(), requestManager, errorMessage);

        action.execute(inputs, timeout, listener);
    }

    @Override
    public void doInfer(
        Model model,
        InferenceInputs inputs,
        Map<String, Object> taskSettings,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        if (model instanceof AnthropicModel == false) {
            listener.onFailure(createInvalidModelException(model));
            return;
        }

        AnthropicModel anthropicModel = (AnthropicModel) model;
        var actionCreator = new AnthropicActionCreator(getSender(), getServiceComponents());

        var action = anthropicModel.accept(actionCreator, taskSettings);
        action.execute(inputs, timeout, listener);
    }

    @Override
    protected void validateInputType(InputType inputType, Model model, ValidationException validationException) {}

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
        throw new UnsupportedOperationException("Anthropic service does not support chunked inference");
    }

    @Override
    protected boolean supportsChunkedInfer() {
        return false;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.minimumCompatible();
    }

    @Override
    public Set<TaskType> supportedStreamingTasks() {
        return EnumSet.of(TaskType.COMPLETION, TaskType.CHAT_COMPLETION);
    }

    public static class Configuration {
        public static InferenceServiceConfiguration get() {
            return CONFIGURATION.getOrCompute();
        }

        private static final LazyInitializable<InferenceServiceConfiguration, RuntimeException> CONFIGURATION = new LazyInitializable<>(
            () -> {
                var configurationMap = new HashMap<String, SettingsConfiguration>();

                configurationMap.put(
                    MODEL_ID,
                    new SettingsConfiguration.Builder(SUPPORTED_TASK_TYPES).setDescription(
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
                    AnthropicServiceFields.MAX_TOKENS,
                    new SettingsConfiguration.Builder(SUPPORTED_TASK_TYPES).setDescription(
                        "The maximum number of tokens to generate before stopping."
                    )
                        .setLabel("Max Tokens")
                        .setRequired(true)
                        .setSensitive(false)
                        .setUpdatable(false)
                        .setType(SettingsConfigurationFieldType.INTEGER)
                        .build()
                );

                configurationMap.putAll(DefaultSecretSettings.toSettingsConfiguration(SUPPORTED_TASK_TYPES));
                configurationMap.putAll(
                    RateLimitSettings.toSettingsConfigurationWithDescription(
                        "By default, the anthropic service sets the number of requests allowed per minute to 50.",
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
