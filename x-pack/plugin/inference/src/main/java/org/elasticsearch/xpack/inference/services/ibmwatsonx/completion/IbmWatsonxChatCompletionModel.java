/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx.completion;

import org.apache.http.client.utils.URIBuilder;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.IbmWatsonxModel;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.IbmWatsonxRateLimitServiceSettings;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.action.IbmWatsonxActionVisitor;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ibmwatsonx.request.IbmWatsonxUtils.COMPLETIONS;
import static org.elasticsearch.xpack.inference.services.ibmwatsonx.request.IbmWatsonxUtils.ML;
import static org.elasticsearch.xpack.inference.services.ibmwatsonx.request.IbmWatsonxUtils.TEXT;
import static org.elasticsearch.xpack.inference.services.ibmwatsonx.request.IbmWatsonxUtils.V1;

public class IbmWatsonxChatCompletionModel extends IbmWatsonxModel {

    /**
     * Constructor for IbmWatsonxChatCompletionModel.
     *
     * @param inferenceEntityId The unique identifier for the inference entity.
     * @param taskType The type of task this model is designed for.
     * @param service The name of the service this model belongs to.
     * @param serviceSettings The settings specific to the Ibm Granite chat completion service.
     * @param secrets The secrets required for accessing the service.
     * @param context The context for parsing configuration settings.
     */
    public IbmWatsonxChatCompletionModel(
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
            IbmWatsonxChatCompletionServiceSettings.fromMap(serviceSettings, context),
            DefaultSecretSettings.fromMap(secrets)
        );
    }

    /**
     * Creates a new IbmWatsonxChatCompletionModel with overridden service settings.
     *
     * @param model The original IbmWatsonxChatCompletionModel.
     * @param request The UnifiedCompletionRequest containing the model override.
     * @return A new IbmWatsonxChatCompletionModel with the overridden model ID.
     */
    public static IbmWatsonxChatCompletionModel of(IbmWatsonxChatCompletionModel model, UnifiedCompletionRequest request) {
        if (request.model() == null) {
            // If no model is specified in the request, return the original model
            return model;
        }

        var originalModelServiceSettings = model.getServiceSettings();
        var overriddenServiceSettings = new IbmWatsonxChatCompletionServiceSettings(
            originalModelServiceSettings.uri(),
            originalModelServiceSettings.apiVersion(),
            originalModelServiceSettings.modelId(),
            originalModelServiceSettings.projectId(),
            originalModelServiceSettings.rateLimitSettings()
        );

        return new IbmWatsonxChatCompletionModel(
            model.getInferenceEntityId(),
            model.getTaskType(),
            model.getConfigurations().getService(),
            overriddenServiceSettings,
            model.getSecretSettings()
        );
    }

    // should only be used for testing
    IbmWatsonxChatCompletionModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        IbmWatsonxChatCompletionServiceSettings serviceSettings,
        @Nullable DefaultSecretSettings secretSettings
    ) {
        super(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings),
            new ModelSecrets(secretSettings),
            serviceSettings
        );
    }

    @Override
    public IbmWatsonxRateLimitServiceSettings rateLimitServiceSettings() {
        return super.rateLimitServiceSettings();
    }

    @Override
    public IbmWatsonxChatCompletionServiceSettings getServiceSettings() {
        return (IbmWatsonxChatCompletionServiceSettings) super.getServiceSettings();
    }

    @Override
    public DefaultSecretSettings getSecretSettings() {
        return (DefaultSecretSettings) super.getSecretSettings();
    }

    public URI uri() {
        URI uri;
        try {
            uri = buildUri(this.getServiceSettings().uri().toString(), this.getServiceSettings().apiVersion());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }

        return uri;
    }

    /**
     * Accepts a visitor to create an executable action. The returned action will not return documents in the response.
     * @param visitor _
     * @return the completion action
     */
    public ExecutableAction accept(IbmWatsonxActionVisitor visitor, Map<String, Object> taskSettings) {
        return visitor.create(this);
    }

    public static URI buildUri(String uri, String apiVersion) throws URISyntaxException {
        return new URIBuilder().setScheme("https")
            .setHost(uri)
            .setPathSegments(ML, V1, TEXT, COMPLETIONS)
            .setParameter("version", apiVersion)
            .build();
    }
}
