/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.nvidia.completion;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.SecretSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.nvidia.NvidiaModel;
import org.elasticsearch.xpack.inference.services.nvidia.action.NvidiaActionVisitor;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.util.Map;

/**
 * Represents an Nvidia chat completion model for inference.
 * This class extends {@link NvidiaModel} and provides specific configurations and settings for chat completion tasks.
 */
public class NvidiaChatCompletionModel extends NvidiaModel {

    /**
     * Constructs a new {@link NvidiaChatCompletionModel} with specified parameters.
     *
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
     * Constructs a new {@link NvidiaChatCompletionModel} with specified parameters.
     *
     * @param inferenceEntityId the unique identifier for the inference entity
     * @param taskType the type of task this model is designed for
     * @param service the name of the inference service
     * @param serviceSettings the settings for the inference service, specific to chat completion
     * @param secrets the secret settings for the model, such as API keys
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
     * Factory method to create an {@link NvidiaChatCompletionModel} with overridden model settings based on the request.
     * If the modelId is null or matches the original model's modelId, the original model is returned.
     *
     * @param model the original {@link NvidiaChatCompletionModel}
     * @param modelId the model identifier specified in the request, or null if not specified
     * @return a new {@link NvidiaChatCompletionModel} with overridden settings or the original model if no overrides are specified
     */
    public static NvidiaChatCompletionModel of(NvidiaChatCompletionModel model, @Nullable String modelId) {
        if (modelId == null || modelId.equals(model.getServiceSettings().modelId())) {
            // If modelId is null or matches the original model's modelId, return the original model
            return model;
        }

        var originalModelServiceSettings = model.getServiceSettings();
        var overriddenServiceSettings = new NvidiaChatCompletionServiceSettings(
            modelId,
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

    /**
     * Returns the service settings specific to Nvidia chat completion.
     *
     * @return the {@link NvidiaChatCompletionServiceSettings} associated with this model
     */
    @Override
    public NvidiaChatCompletionServiceSettings getServiceSettings() {
        return (NvidiaChatCompletionServiceSettings) super.getServiceSettings();
    }

    /**
     * Accepts a visitor that creates an executable action for this Nvidia chat completion model.
     *
     * @param creator the visitor that creates the executable action
     * @param taskSettings the task settings for the inference task (not used in this model)
     * @return an {@link ExecutableAction} representing the Nvidia chat completion model
     */
    @Override
    public ExecutableAction accept(NvidiaActionVisitor creator, Map<String, Object> taskSettings) {
        return creator.create(this);
    }
}
