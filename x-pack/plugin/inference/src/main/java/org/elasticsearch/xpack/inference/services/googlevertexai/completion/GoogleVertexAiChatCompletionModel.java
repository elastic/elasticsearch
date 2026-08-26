/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.completion;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiModel;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiRateLimitServiceSettings;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiSecretSettings;
import org.elasticsearch.xpack.inference.services.googlevertexai.action.GoogleVertexAiActionVisitor;
import org.elasticsearch.xpack.inference.services.googlevertexai.request.GoogleVertexAiUtils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;

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

    // should be used directly only for testing
    GoogleVertexAiChatCompletionModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        GoogleVertexAiChatCompletionServiceSettings serviceSettings,
        GoogleVertexAiChatCompletionTaskSettings taskSettings,
        @Nullable GoogleVertexAiSecretSettings secrets
    ) {
        this(new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings), new ModelSecrets(secrets));
    }

    // Should only be used for testing
    GoogleVertexAiChatCompletionModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        GoogleVertexAiChatCompletionServiceSettings serviceSettings,
        GoogleVertexAiChatCompletionTaskSettings taskSettings,
        @Nullable GoogleVertexAiSecretSettings secrets,
        BiConsumer<HttpPost, GoogleVertexAiModel> authHeaderDecorator
    ) {
        this(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings),
            new ModelSecrets(secrets),
            authHeaderDecorator
        );
    }

    public GoogleVertexAiChatCompletionModel(ModelConfigurations modelConfigurations, ModelSecrets modelSecrets) {
        super(modelConfigurations, modelSecrets, (GoogleVertexAiRateLimitServiceSettings) modelConfigurations.getServiceSettings());
        var resolvedUris = resolveUris(modelConfigurations);
        this.streamingURI = resolvedUris.streaming();
        this.nonStreamingUri = resolvedUris.nonStreaming();
    }

    // Should only be used for testing
    public GoogleVertexAiChatCompletionModel(
        ModelConfigurations modelConfigurations,
        ModelSecrets modelSecrets,
        BiConsumer<HttpPost, GoogleVertexAiModel> authHeaderDecorator
    ) {
        super(
            modelConfigurations,
            modelSecrets,
            (GoogleVertexAiRateLimitServiceSettings) modelConfigurations.getServiceSettings(),
            authHeaderDecorator
        );
        var resolvedUris = resolveUris(modelConfigurations);
        this.streamingURI = resolvedUris.streaming();
        this.nonStreamingUri = resolvedUris.nonStreaming();
    }

    private record ResolvedUris(URI streaming, URI nonStreaming) {}

    private ResolvedUris resolveUris(ModelConfigurations modelConfigurations) {
        URI streamingUri;
        URI nonStreamingUri;
        try {
            var serviceSettings = (GoogleVertexAiChatCompletionServiceSettings) modelConfigurations.getServiceSettings();
            var uri = serviceSettings.uri();
            streamingUri = serviceSettings.streamingUri();
            // For Google Model Garden uri or streamingUri must be set. If not - location, projectId and modelId must be set
            if (uri != null || streamingUri != null) {
                // If both uris are provided, each will be used as-is (non-streaming vs. streaming).
                // If only one is provided, it will be reused for both non-streaming and streaming requests.
                // Some providers require both (e.g. Anthropic, Mistral, Ai21).
                // Some providers work fine with a single URL (e.g. Meta, Hugging Face).
                nonStreamingUri = Objects.requireNonNullElse(uri, streamingUri);
                streamingUri = Objects.requireNonNullElse(streamingUri, uri);
            } else {
                // If neither uri nor streamingUri is provided, build them from location, projectId, and modelId.
                var location = serviceSettings.location();
                var projectId = serviceSettings.projectId();
                var model = serviceSettings.modelId();
                streamingUri = buildStreamingUri(location, projectId, model);
                nonStreamingUri = buildNonStreamingUri(location, projectId, model);
            }
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
        return new ResolvedUris(streamingUri, nonStreamingUri);
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

        var newTaskSettings = GoogleVertexAiChatCompletionTaskSettings.fromMap(taskSettingsMap);
        if (newTaskSettings.isEmpty() || model.getTaskSettings().equals(newTaskSettings)) {
            return model;
        }

        var combinedTaskSettings = GoogleVertexAiChatCompletionTaskSettings.of(model.getTaskSettings(), newTaskSettings);
        return new GoogleVertexAiChatCompletionModel(model, combinedTaskSettings);
    }

    @Override
    public ExecutableAction accept(GoogleVertexAiActionVisitor visitor, Map<String, Object> taskSettings) {
        return visitor.create(this, taskSettings);
    }

    @Override
    public GoogleVertexAiRateLimitServiceSettings rateLimitServiceSettings() {
        return super.rateLimitServiceSettings();
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

    public static URI buildNonStreamingUri(@Nullable String location, String projectId, String model) throws URISyntaxException {
        return new URIBuilder().setScheme("https")
            .setHost(GoogleVertexAiUtils.resolveHost(location))
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

    public static URI buildStreamingUri(@Nullable String location, String projectId, String model) throws URISyntaxException {
        return new URIBuilder().setScheme("https")
            .setHost(GoogleVertexAiUtils.resolveHost(location))
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
