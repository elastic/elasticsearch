/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mistral.completion;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.mistral.MistralModel;
import org.elasticsearch.xpack.inference.services.mistral.action.MistralActionVisitor;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.mistral.MistralConstants.API_COMPLETIONS_PATH;

/**
 * Represents a Mistral chat completion model.
 * This class extends RateLimitGroupingModel to handle rate limiting based on model and API key.
 */
public class MistralChatCompletionModel extends MistralModel {

    /**
     * Constructor for MistralChatCompletionModel.
     *
     * @param inferenceEntityId The unique identifier for the inference entity.
     * @param taskType The type of task this model is designed for.
     * @param service The name of the service this model belongs to.
     * @param serviceSettings The settings specific to the Mistral chat completion service.
     * @param secrets The secrets required for accessing the service.
     * @param context The context for parsing configuration settings.
     */
    public MistralChatCompletionModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        @Nullable Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            inferenceEntityId,
            taskType,
            service,
            MistralChatCompletionServiceSettings.fromMap(serviceSettings, context),
            DefaultSecretSettings.fromMap(secrets)
        );
    }

    /**
     * Creates a new MistralChatCompletionModel with overridden service settings.
     *
     * @param model The original MistralChatCompletionModel.
     * @param request The UnifiedCompletionRequest containing the model override.
     * @return A new MistralChatCompletionModel with the overridden model ID.
     */
    public static MistralChatCompletionModel of(MistralChatCompletionModel model, UnifiedCompletionRequest request) {
        if (request.model() == null) {
            // If no model is specified in the request, return the original model
            return model;
        }

        var originalModelServiceSettings = model.getServiceSettings();
        var overriddenServiceSettings = new MistralChatCompletionServiceSettings(
            request.model(),
            originalModelServiceSettings.rateLimitSettings()
        );

        return new MistralChatCompletionModel(
            model.getInferenceEntityId(),
            model.getTaskType(),
            model.getConfigurations().getService(),
            overriddenServiceSettings,
            model.getSecretSettings()
        );
    }

    public MistralChatCompletionModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        MistralChatCompletionServiceSettings serviceSettings,
        DefaultSecretSettings secrets
    ) {
        super(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, new EmptyTaskSettings()),
            new ModelSecrets(secrets)
        );
        setPropertiesFromServiceSettings(serviceSettings);
    }

    private void setPropertiesFromServiceSettings(MistralChatCompletionServiceSettings serviceSettings) {
        this.model = serviceSettings.modelId();
        this.rateLimitSettings = serviceSettings.rateLimitSettings();
        setEndpointUrl();
    }

    @Override
    public int rateLimitGroupingHash() {
        return Objects.hash(model, getSecretSettings().apiKey());
    }

    private void setEndpointUrl() {
        try {
            this.uri = new URI(API_COMPLETIONS_PATH);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public MistralChatCompletionServiceSettings getServiceSettings() {
        return (MistralChatCompletionServiceSettings) super.getServiceSettings();
    }

    /**
     * Accepts a visitor to create an executable action for this model.
     *
     * @param creator The visitor that creates the executable action.
     * @return An ExecutableAction that can be executed.
     */
    public ExecutableAction accept(MistralActionVisitor creator) {
        return creator.create(this);
    }
}
