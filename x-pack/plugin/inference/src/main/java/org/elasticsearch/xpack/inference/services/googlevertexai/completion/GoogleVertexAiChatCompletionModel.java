/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.completion;

import org.apache.http.client.utils.URIBuilder;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleModelGardenProvider;
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
            new EmptyTaskSettings(),
            GoogleVertexAiSecretSettings.fromMap(secrets)
        );
    }

    GoogleVertexAiChatCompletionModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        GoogleVertexAiChatCompletionServiceSettings serviceSettings,
        EmptyTaskSettings taskSettings,
        @Nullable GoogleVertexAiSecretSettings secrets
    ) {
        super(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings),
            new ModelSecrets(secrets),
            serviceSettings
        );
        try {
            GoogleModelGardenProvider provider = serviceSettings.provider();
            URI uri = serviceSettings.uri();
            if (provider != null && uri != null) {
                this.nonStreamingUri = uri;
                this.streamingURI = Objects.requireNonNullElse(serviceSettings.streamingUri(), uri);
            } else {
                String location = serviceSettings.location();
                String projectId = serviceSettings.projectId();
                String model = serviceSettings.modelId();
                this.streamingURI = buildUriStreaming(location, projectId, model);
                this.nonStreamingUri = buildUriNonStreaming(location, projectId, model);
            }
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public static GoogleVertexAiChatCompletionModel of(GoogleVertexAiChatCompletionModel model, UnifiedCompletionRequest request) {
        var originalModelServiceSettings = model.getServiceSettings();

        var newServiceSettings = new GoogleVertexAiChatCompletionServiceSettings(
            originalModelServiceSettings.projectId(),
            originalModelServiceSettings.location(),
            request.model() != null ? request.model() : originalModelServiceSettings.modelId(),
            originalModelServiceSettings.uri(),
            originalModelServiceSettings.streamingUri(),
            originalModelServiceSettings.provider(),
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
    public EmptyTaskSettings getTaskSettings() {
        return (EmptyTaskSettings) super.getTaskSettings();
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
        var uri = getServiceSettings().uri();
        var streamingUri = getServiceSettings().streamingUri();
        var provider = getServiceSettings().provider();

        // Since we don't beforehand know which API is going to be used, we take a conservative approach and
        // count both endpoint for the rate limit
        return Objects.hash(
            projectId,
            location,
            modelId,
            uri,
            streamingUri,
            provider,
            GoogleVertexAiUtils.GENERATE_CONTENT,
            GoogleVertexAiUtils.STREAM_GENERATE_CONTENT
        );
    }
}
