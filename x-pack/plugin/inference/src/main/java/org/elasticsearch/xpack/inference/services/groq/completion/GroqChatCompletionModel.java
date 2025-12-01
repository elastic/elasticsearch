/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.groq.completion;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.SecretSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.groq.GroqModel;
import org.elasticsearch.xpack.inference.services.groq.action.GroqActionVisitor;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.util.Map;

/**
 * Represents a Groq chat completion model for inference.
 * This class extends {@link GroqModel} and provides specific configurations and settings for chat completion tasks.
 */
public class GroqChatCompletionModel extends GroqModel {

    /**
     * Constructs a new {@link GroqChatCompletionModel} with specified parameters.
     *
     * @param inferenceEntityId the unique identifier for the inference entity
     * @param taskType the type of task this model is designed for
     * @param service the name of the inference service
     * @param serviceSettings the settings for the inference service, specific to chat completion
     * @param secrets the secret settings for the model, such as API keys or tokens
     * @param context the context for parsing configuration settings
     */
    public GroqChatCompletionModel(
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
            GroqChatCompletionServiceSettings.fromMap(serviceSettings, context),
            DefaultSecretSettings.fromMap(secrets)
        );
    }

    /**
     * Constructs a new {@link GroqChatCompletionModel} with specified parameters.
     *
     * @param inferenceEntityId the unique identifier for the inference entity
     * @param taskType the type of task this model is designed for
     * @param service the name of the inference service
     * @param serviceSettings the settings for the inference service, specific to chat completion
     * @param secrets the secret settings for the model, such as API keys
     */
    public GroqChatCompletionModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        GroqChatCompletionServiceSettings serviceSettings,
        SecretSettings secrets
    ) {
        super(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, EmptyTaskSettings.INSTANCE),
            new ModelSecrets(secrets)
        );
    }

    /**
     * Factory method to create a {@link GroqChatCompletionModel} with overridden model settings based on the request.
     * If the modelId is null or matches the original model's modelId, the original model is returned.
     *
     * @param model the original {@link GroqChatCompletionModel}
     * @param modelId the model identifier specified in the request, or null if not specified
     * @return a new {@link GroqChatCompletionModel} with overridden settings or the original model if no overrides are specified
     */
    public static GroqChatCompletionModel of(GroqChatCompletionModel model, @Nullable String modelId) {
        if (modelId == null || modelId.equals(model.getServiceSettings().modelId())) {
            // If modelId is null or matches the original model's modelId, return the original model
            return model;
        }

        var originalModelServiceSettings = model.getServiceSettings();
        var overriddenServiceSettings = new GroqChatCompletionServiceSettings(modelId, originalModelServiceSettings.rateLimitSettings());

        return new GroqChatCompletionModel(
            model.getInferenceEntityId(),
            model.getTaskType(),
            model.getConfigurations().getService(),
            overriddenServiceSettings,
            model.getSecretSettings()
        );
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return getServiceSettings().rateLimitSettings();
    }

    /**
     * Returns the service settings specific to Groq chat completion.
     *
     * @return the {@link GroqChatCompletionServiceSettings} associated with this model
     */
    @Override
    public GroqChatCompletionServiceSettings getServiceSettings() {
        return (GroqChatCompletionServiceSettings) super.getServiceSettings();
    }

    /**
     * Accepts a visitor that creates an executable action for this Groq chat completion model.
     *
     * @param creator the visitor that creates the executable action
     * @return an {@link ExecutableAction} representing the Groq chat completion model
     */
    @Override
    public ExecutableAction accept(GroqActionVisitor creator) {
        return creator.create(this);
    }
}
