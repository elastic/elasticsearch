/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.completion;

import org.apache.http.client.utils.URIBuilder;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiModel;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiSecretSettings;
import org.elasticsearch.xpack.inference.services.googlevertexai.action.GoogleVertexAiActionVisitor;
import org.elasticsearch.xpack.inference.services.googlevertexai.request.GoogleVertexAiUtils;
import org.elasticsearch.xpack.inference.services.googlevertexai.rerank.GoogleDiscoveryEngineRateLimitServiceSettings;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.core.Strings.format;

public class GoogleVertexAiChatCompletionModel extends GoogleVertexAiModel {

    private final URI streamingURI;

    public GoogleVertexAiChatCompletionModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            inferenceEntityId,
            taskType,
            service,
            GoogleVertexAiChatCompletionServiceSettings.fromMap(serviceSettings, context),
            GoogleVertexAiChatCompletionTaskSettings.fromMap(taskSettings),
            GoogleVertexAiSecretSettings.fromMap(secrets)
        );
    }

    GoogleVertexAiChatCompletionModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        GoogleVertexAiChatCompletionServiceSettings serviceSettings,
        GoogleVertexAiChatCompletionTaskSettings taskSettings,
        @Nullable GoogleVertexAiSecretSettings secrets
    ) {
        super(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings),
            new ModelSecrets(secrets),
            serviceSettings
        );
        try {
            this.streamingURI = buildUriStreaming(serviceSettings.location(), serviceSettings.projectId(), serviceSettings.modelId());
            this.nonStreamingUri = buildUriNonStreaming(serviceSettings.location(), serviceSettings.projectId(), serviceSettings.modelId());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private GoogleVertexAiChatCompletionModel(
        GoogleVertexAiChatCompletionModel model,
        GoogleVertexAiChatCompletionTaskSettings taskSettings
    ) {
        super(model, taskSettings);
        streamingURI = model.streamingURI();
    }

    public static GoogleVertexAiChatCompletionModel of(GoogleVertexAiChatCompletionModel model, UnifiedCompletionRequest request) {
        var originalModelServiceSettings = model.getServiceSettings();

        var newServiceSettings = new GoogleVertexAiChatCompletionServiceSettings(
            originalModelServiceSettings.projectId(),
            originalModelServiceSettings.location(),
            Objects.requireNonNullElse(request.model(), originalModelServiceSettings.modelId()),
            originalModelServiceSettings.rateLimitSettings()
        );

        return new GoogleVertexAiChatCompletionModel(
            model.getInferenceEntityId(),
            model.getTaskType(),
            model.getConfigurations().getService(),
            newServiceSettings,
            model.getTaskSettings(),
            model.getSecretSettings()
        );
    }

    /**
     * Overrides the task settings in the given model with the settings in the map. If no new settings are present or the provided settings
     * do not differ from those already in the model, returns the original model
     * @param model the model whose task settings will be overridden
     * @param taskSettingsMap the new task settings to use
     * @return a {@link GoogleVertexAiChatCompletionModel} with overridden {@link GoogleVertexAiChatCompletionTaskSettings}
     */
    public static GoogleVertexAiChatCompletionModel of(GoogleVertexAiChatCompletionModel model, Map<String, Object> taskSettingsMap) {
        if (taskSettingsMap == null || taskSettingsMap.isEmpty()) {
            return model;
        }

        var requestTaskSettings = GoogleVertexAiChatCompletionTaskSettings.fromMap(taskSettingsMap);
        if (requestTaskSettings.isEmpty() || model.getTaskSettings().equals(requestTaskSettings)) {
            return model;
        }
        var combinedTaskSettings = GoogleVertexAiChatCompletionTaskSettings.of(model.getTaskSettings(), requestTaskSettings);
        return new GoogleVertexAiChatCompletionModel(model, combinedTaskSettings);
    }

    @Override
    public ExecutableAction accept(GoogleVertexAiActionVisitor visitor, Map<String, Object> taskSettings) {
        return visitor.create(this, taskSettings);
    }

    @Override
    public GoogleDiscoveryEngineRateLimitServiceSettings rateLimitServiceSettings() {
        return (GoogleDiscoveryEngineRateLimitServiceSettings) super.rateLimitServiceSettings();
    }

    @Override
    public GoogleVertexAiChatCompletionServiceSettings getServiceSettings() {
        return (GoogleVertexAiChatCompletionServiceSettings) super.getServiceSettings();
    }

    @Override
    public GoogleVertexAiChatCompletionTaskSettings getTaskSettings() {
        return (GoogleVertexAiChatCompletionTaskSettings) super.getTaskSettings();
    }

    @Override
    public GoogleVertexAiSecretSettings getSecretSettings() {
        return (GoogleVertexAiSecretSettings) super.getSecretSettings();
    }

    public URI streamingURI() {
        return this.streamingURI;
    }

    public static URI buildUriNonStreaming(String location, String projectId, String model) throws URISyntaxException {
        return new URIBuilder().setScheme("https")
            .setHost(format("%s%s", location, GoogleVertexAiUtils.GOOGLE_VERTEX_AI_HOST_SUFFIX))
            .setPathSegments(
                GoogleVertexAiUtils.V1,
                GoogleVertexAiUtils.PROJECTS,
                projectId,
                GoogleVertexAiUtils.LOCATIONS,
                GoogleVertexAiUtils.GLOBAL,
                GoogleVertexAiUtils.PUBLISHERS,
                GoogleVertexAiUtils.PUBLISHER_GOOGLE,
                GoogleVertexAiUtils.MODELS,
                format("%s:%s", model, GoogleVertexAiUtils.GENERATE_CONTENT)
            )
            .build();
    }

    public static URI buildUriStreaming(String location, String projectId, String model) throws URISyntaxException {
        return new URIBuilder().setScheme("https")
            .setHost(format("%s%s", location, GoogleVertexAiUtils.GOOGLE_VERTEX_AI_HOST_SUFFIX))
            .setPathSegments(
                GoogleVertexAiUtils.V1,
                GoogleVertexAiUtils.PROJECTS,
                projectId,
                GoogleVertexAiUtils.LOCATIONS,
                GoogleVertexAiUtils.GLOBAL,
                GoogleVertexAiUtils.PUBLISHERS,
                GoogleVertexAiUtils.PUBLISHER_GOOGLE,
                GoogleVertexAiUtils.MODELS,
                format("%s:%s", model, GoogleVertexAiUtils.STREAM_GENERATE_CONTENT)
            )
            .setCustomQuery(GoogleVertexAiUtils.QUERY_PARAM_ALT_SSE)
            .build();
    }

    @Override
    public int rateLimitGroupingHash() {
        // In VertexAI rate limiting is scoped to the project, region, model and endpoint.
        // API Key does not affect the quota
        // https://ai.google.dev/gemini-api/docs/rate-limits
        // https://cloud.google.com/vertex-ai/docs/quotas
        var projectId = getServiceSettings().projectId();
        var location = getServiceSettings().location();
        var modelId = getServiceSettings().modelId();

        // Since we don't beforehand know which API is going to be used, we take a conservative approach and
        // count both endpoint for the rate limit
        return Objects.hash(
            projectId,
            location,
            modelId,
            GoogleVertexAiUtils.GENERATE_CONTENT,
            GoogleVertexAiUtils.STREAM_GENERATE_CONTENT
        );
    }
}
