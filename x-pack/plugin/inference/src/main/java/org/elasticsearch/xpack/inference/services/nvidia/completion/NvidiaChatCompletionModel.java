/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.nvidia.completion;

import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.SecretSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.nvidia.NvidiaModel;
import org.elasticsearch.xpack.inference.services.nvidia.action.NvidiaActionVisitor;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.util.Map;
import java.util.Objects;

/**
 * Represents a Nvidia chat completion model for inference.
 * This class extends the NvidiaModel and provides specific configurations and settings for chat completion tasks.
 */
public class NvidiaChatCompletionModel extends NvidiaModel {

    /**
     * Constructor for creating a NvidiaChatCompletionModel with specified parameters.
     * @param inferenceEntityId the unique identifier for the inference entity
     * @param taskType the type of task this model is designed for
     * @param service the name of the inference service
     * @param serviceSettings the settings for the inference service, specific to chat completion
     * @param secrets the secret settings for the model, such as API keys or tokens
     * @param context the context for parsing configuration settings
     */
    public NvidiaChatCompletionModel(
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
            NvidiaChatCompletionServiceSettings.fromMap(serviceSettings, context),
            DefaultSecretSettings.fromMap(secrets)
        );
    }

    /**
     * Constructor for creating a NvidiaChatCompletionModel with specified parameters.
     * @param inferenceEntityId the unique identifier for the inference entity
     * @param taskType the type of task this model is designed for
     * @param service the name of the inference service
     * @param serviceSettings the settings for the inference service, specific to chat completion
     * @param secrets the secret settings for the model, such as API keys or tokens
     */
    public NvidiaChatCompletionModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        NvidiaChatCompletionServiceSettings serviceSettings,
        SecretSettings secrets
    ) {
        super(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, EmptyTaskSettings.INSTANCE),
            new ModelSecrets(secrets)
        );
    }

    /**
     * Factory method to create a NvidiaChatCompletionModel with overridden model settings based on the request.
     * If the request does not specify a model, the original model is returned.
     *
     * @param model the original NvidiaChatCompletionModel
     * @param request the UnifiedCompletionRequest containing potential overrides
     * @return a new NvidiaChatCompletionModel with overridden settings or the original model if no overrides are specified
     */
    public static NvidiaChatCompletionModel of(NvidiaChatCompletionModel model, UnifiedCompletionRequest request) {
        if (request.model() == null) {
            // If no model id is specified in the request, return the original model
            return model;
        }

        var originalModelServiceSettings = model.getServiceSettings();
        var overriddenServiceSettings = new NvidiaChatCompletionServiceSettings(
            request.model(),
            originalModelServiceSettings.uri(),
            originalModelServiceSettings.rateLimitSettings()
        );

        return new NvidiaChatCompletionModel(
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

    @Override
    public int rateLimitGroupingHash() {
        return Objects.hash(getServiceSettings().modelId(), getServiceSettings().uri(), getSecretSettings().apiKey());
    }

    /**
     * Returns the service settings specific to Nvidia chat completion.
     *
     * @return the NvidiaChatCompletionServiceSettings associated with this model
     */
    @Override
    public NvidiaChatCompletionServiceSettings getServiceSettings() {
        return (NvidiaChatCompletionServiceSettings) super.getServiceSettings();
    }

    /**
     * Accepts a visitor that creates an executable action for this Nvidia chat completion model.
     *
     * @param creator the visitor that creates the executable action
     * @return an ExecutableAction representing this model
     */
    public ExecutableAction accept(NvidiaActionVisitor creator) {
        return creator.create(this);
    }
}
