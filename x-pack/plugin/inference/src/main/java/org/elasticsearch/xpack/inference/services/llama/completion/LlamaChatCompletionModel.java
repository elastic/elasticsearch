/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.llama.completion;

import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.SecretSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.llama.LlamaModel;
import org.elasticsearch.xpack.inference.services.llama.action.LlamaActionVisitor;

import java.util.Map;

/**
 * Represents a Llama chat completion model for inference.
 * This class extends the LlamaModel and provides specific configurations and settings for chat completion tasks.
 */
public class LlamaChatCompletionModel extends LlamaModel {

    /**
     * Constructor for creating a LlamaChatCompletionModel with specified parameters.
     * @param inferenceEntityId the unique identifier for the inference entity
     * @param taskType the type of task this model is designed for
     * @param service the name of the inference service
     * @param serviceSettings the settings for the inference service, specific to chat completion
     * @param secrets the secret settings for the model, such as API keys or tokens
     * @param context the context for parsing configuration settings
     */
    public LlamaChatCompletionModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            inferenceEntityId,
            taskType,
            service,
            LlamaChatCompletionServiceSettings.fromMap(serviceSettings, context),
            retrieveSecretSettings(secrets)
        );
    }

    /**
     * Constructor for creating a LlamaChatCompletionModel with specified parameters.
     * @param inferenceEntityId the unique identifier for the inference entity
     * @param taskType the type of task this model is designed for
     * @param service the name of the inference service
     * @param serviceSettings the settings for the inference service, specific to chat completion
     * @param secrets the secret settings for the model, such as API keys or tokens
     */
    public LlamaChatCompletionModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        LlamaChatCompletionServiceSettings serviceSettings,
        SecretSettings secrets
    ) {
        super(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, new EmptyTaskSettings()),
            new ModelSecrets(secrets)
        );
        setPropertiesFromServiceSettings(serviceSettings);
    }

    /**
     * Factory method to create a LlamaChatCompletionModel with overridden model settings based on the request.
     * If the request does not specify a model, the original model is returned.
     *
     * @param model the original LlamaChatCompletionModel
     * @param request the UnifiedCompletionRequest containing potential overrides
     * @return a new LlamaChatCompletionModel with overridden settings or the original model if no overrides are specified
     */
    public static LlamaChatCompletionModel of(LlamaChatCompletionModel model, UnifiedCompletionRequest request) {
        if (request.model() == null) {
            // If no model id is specified in the request, return the original model
            return model;
        }

        var originalModelServiceSettings = model.getServiceSettings();
        var overriddenServiceSettings = new LlamaChatCompletionServiceSettings(
            request.model(),
            originalModelServiceSettings.uri(),
            originalModelServiceSettings.rateLimitSettings()
        );

        return new LlamaChatCompletionModel(
            model.getInferenceEntityId(),
            model.getTaskType(),
            model.getConfigurations().getService(),
            overriddenServiceSettings,
            model.getSecretSettings()
        );
    }

    private void setPropertiesFromServiceSettings(LlamaChatCompletionServiceSettings serviceSettings) {
        this.modelId = serviceSettings.modelId();
        this.uri = serviceSettings.uri();
        this.rateLimitSettings = serviceSettings.rateLimitSettings();
    }

    /**
     * Returns the service settings specific to Llama chat completion.
     *
     * @return the LlamaChatCompletionServiceSettings associated with this model
     */
    @Override
    public LlamaChatCompletionServiceSettings getServiceSettings() {
        return (LlamaChatCompletionServiceSettings) super.getServiceSettings();
    }

    /**
     * Accepts a visitor that creates an executable action for this Llama chat completion model.
     *
     * @param creator the visitor that creates the executable action
     * @return an ExecutableAction representing this model
     */
    public ExecutableAction accept(LlamaActionVisitor creator) {
        return creator.create(this);
    }
}
