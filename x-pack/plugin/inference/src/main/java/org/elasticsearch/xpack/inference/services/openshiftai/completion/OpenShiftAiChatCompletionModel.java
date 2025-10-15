/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openshiftai.completion;

import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.SecretSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.openshiftai.OpenShiftAiModel;
import org.elasticsearch.xpack.inference.services.openshiftai.action.OpenShiftAiActionVisitor;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.util.Map;

/**
 * Represents an OpenShift AI chat completion model.
 * This class extends the OpenShiftAiModel and provides specific configurations for chat completion tasks.
 */
public class OpenShiftAiChatCompletionModel extends OpenShiftAiModel {

    /**
     * Constructor for creating a OpenShiftAiChatCompletionModel with specified parameters.
     * @param inferenceEntityId the unique identifier for the inference entity
     * @param taskType the type of task this model is designed for
     * @param service the name of the inference service
     * @param serviceSettings the settings for the inference service, specific to chat completion
     * @param secrets the secret settings for the model, such as API keys or tokens
     * @param context the context for parsing configuration settings
     */
    public OpenShiftAiChatCompletionModel(
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
            OpenShiftAiChatCompletionServiceSettings.fromMap(serviceSettings, context),
            DefaultSecretSettings.fromMap(secrets)
        );
    }

    /**
     * Constructor for creating an OpenShiftAiChatCompletionModel with specified parameters.
     * @param inferenceEntityId the unique identifier for the inference entity
     * @param taskType the type of task this model is designed for
     * @param service the name of the inference service
     * @param serviceSettings the settings for the inference service, specific to chat completion
     * @param secrets the secret settings for the model
     */
    public OpenShiftAiChatCompletionModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        OpenShiftAiChatCompletionServiceSettings serviceSettings,
        SecretSettings secrets
    ) {
        super(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, EmptyTaskSettings.INSTANCE),
            new ModelSecrets(secrets)
        );
    }

    /**
     * Factory method to create an OpenShiftAiChatCompletionModel with potential overrides from a UnifiedCompletionRequest.
     * If the request does not specify a model ID, the original model is returned.
     *
     * @param model the original OpenShiftAiChatCompletionModel
     * @param request the UnifiedCompletionRequest containing potential overrides
     * @return a new OpenShiftAiChatCompletionModel with overridden settings or the original model ID if no overrides are specified
     */
    public static OpenShiftAiChatCompletionModel of(OpenShiftAiChatCompletionModel model, UnifiedCompletionRequest request) {
        if (request.model() == null) {
            // If no model ID is specified in the request, return the original model
            return model;
        }

        var originalModelServiceSettings = model.getServiceSettings();
        var overriddenServiceSettings = new OpenShiftAiChatCompletionServiceSettings(
            request.model(),
            originalModelServiceSettings.uri(),
            originalModelServiceSettings.rateLimitSettings()
        );

        return new OpenShiftAiChatCompletionModel(
            model.getInferenceEntityId(),
            model.getTaskType(),
            model.getConfigurations().getService(),
            overriddenServiceSettings,
            model.getSecretSettings()
        );
    }

    /**
     * Returns the service settings specific to OpenShift AI chat completion.
     *
     * @return the OpenShiftAiChatCompletionServiceSettings associated with this model
     */
    @Override
    public OpenShiftAiChatCompletionServiceSettings getServiceSettings() {
        return (OpenShiftAiChatCompletionServiceSettings) super.getServiceSettings();
    }

    /**
     * Accepts a visitor that creates an executable action for this OpenShift AI chat completion.
     *
     * @param creator the visitor that creates the executable action
     * @return an ExecutableAction representing this model
     */
    public ExecutableAction accept(OpenShiftAiActionVisitor creator) {
        return creator.create(this);
    }
}
