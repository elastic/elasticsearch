/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ai21.completion;

import org.apache.http.client.utils.URIBuilder;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ai21.Ai21Model;
import org.elasticsearch.xpack.inference.services.ai21.action.Ai21ActionVisitor;
import org.elasticsearch.xpack.inference.services.ai21.request.Ai21ApiConstants;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.net.URISyntaxException;
import java.util.Map;

/**
 * Represents a AI21 chat completion model.
 * This class extends RateLimitGroupingModel to handle rate limiting based on model and API key.
 */
public class Ai21ChatCompletionModel extends Ai21Model {
    /**
     * Constructor for Ai21ChatCompletionModel.
     *
     * @param inferenceEntityId The unique identifier for the inference entity.
     * @param taskType The type of task this model is designed for.
     * @param service The name of the service this model belongs to.
     * @param serviceSettings The settings specific to the AI21 chat completion service.
     * @param secrets The secrets required for accessing the service.
     * @param context The context for parsing configuration settings.
     */
    public Ai21ChatCompletionModel(
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
            Ai21ChatCompletionServiceSettings.fromMap(serviceSettings, context),
            DefaultSecretSettings.fromMap(secrets)
        );
    }

    /**
     * Creates a new Ai21ChatCompletionModel with overridden service settings.
     *
     * @param model The original Ai21ChatCompletionModel.
     * @param request The UnifiedCompletionRequest containing the model override.
     * @return A new Ai21ChatCompletionModel with the overridden model ID.
     */
    public static Ai21ChatCompletionModel of(Ai21ChatCompletionModel model, UnifiedCompletionRequest request) {
        if (request.model() == null) {
            // If no model is specified in the request, return the original model
            return model;
        }

        var originalModelServiceSettings = model.getServiceSettings();
        var overriddenServiceSettings = new Ai21ChatCompletionServiceSettings(
            request.model(),
            originalModelServiceSettings.rateLimitSettings()
        );

        return new Ai21ChatCompletionModel(
            model.getInferenceEntityId(),
            model.getTaskType(),
            model.getConfigurations().getService(),
            overriddenServiceSettings,
            model.getSecretSettings()
        );
    }

    public Ai21ChatCompletionModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        Ai21ChatCompletionServiceSettings serviceSettings,
        DefaultSecretSettings secrets
    ) {
        super(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, new EmptyTaskSettings()),
            new ModelSecrets(secrets)
        );
        setPropertiesFromServiceSettings(serviceSettings);
    }

    private void setPropertiesFromServiceSettings(Ai21ChatCompletionServiceSettings serviceSettings) {
        this.rateLimitSettings = serviceSettings.rateLimitSettings();
        setEndpointUrl();
    }

    private void setEndpointUrl() {
        try {
            this.uri = new URIBuilder().setScheme("https")
                .setHost(Ai21ApiConstants.HOST)
                .setPathSegments(
                    Ai21ApiConstants.STUDIO_PATH,
                    Ai21ApiConstants.VERSION_1,
                    Ai21ApiConstants.CHAT_PATH,
                    Ai21ApiConstants.COMPLETIONS_PATH
                )
                .build();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Ai21ChatCompletionServiceSettings getServiceSettings() {
        return (Ai21ChatCompletionServiceSettings) super.getServiceSettings();
    }

    /**
     * Accepts a visitor to create an executable action for this model.
     *
     * @param creator The visitor that creates the executable action.
     * @return An ExecutableAction that can be executed.
     */
    public ExecutableAction accept(Ai21ActionVisitor creator) {
        return creator.create(this);
    }
}
