/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.completion;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleModelGardenProvider;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.googlevertexai.completion.ThinkingConfig.THINKING_BUDGET_FIELD;
import static org.elasticsearch.xpack.inference.services.googlevertexai.completion.ThinkingConfig.THINKING_CONFIG_FIELD;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class GoogleVertexAiChatCompletionModelTests extends ESTestCase {

    private static final String DEFAULT_PROJECT_ID = "test-project";
    private static final String DEFAULT_LOCATION = "us-central1";
    private static final String DEFAULT_MODEL_ID = "gemini-pro";
    private static final String DEFAULT_API_KEY = "test-api-key";
    private static final RateLimitSettings DEFAULT_RATE_LIMIT = new RateLimitSettings(100);
    private static final ThinkingConfig EMPTY_THINKING_CONFIG = new ThinkingConfig();

    public void testOverrideWith_UnifiedCompletionRequest_OverridesModelId() {
        var model = createCompletionModel(
            DEFAULT_PROJECT_ID,
            DEFAULT_LOCATION,
            DEFAULT_MODEL_ID,
            DEFAULT_API_KEY,
            DEFAULT_RATE_LIMIT,
            EMPTY_THINKING_CONFIG,
            null,
            null,
            null
        );
        var request = new UnifiedCompletionRequest(
            List.of(new UnifiedCompletionRequest.Message(new UnifiedCompletionRequest.ContentString("hello"), "user", null, null)),
            "gemini-flash",
            null,
            null,
            null,
            null,
            null,
            null
        );

        var overriddenModel = GoogleVertexAiChatCompletionModel.of(model, request);

        assertThat(overriddenModel.getServiceSettings().modelId(), is("gemini-flash"));

        assertThat(overriddenModel, not(sameInstance(model)));
        assertThat(overriddenModel.getServiceSettings().projectId(), is(DEFAULT_PROJECT_ID));
        assertThat(overriddenModel.getServiceSettings().location(), is(DEFAULT_LOCATION));
        assertThat(overriddenModel.getServiceSettings().rateLimitSettings(), is(DEFAULT_RATE_LIMIT));
        assertThat(overriddenModel.getSecretSettings().serviceAccountJson(), equalTo(new SecureString(DEFAULT_API_KEY.toCharArray())));
        assertThat(overriddenModel.getTaskSettings().thinkingConfig(), is(EMPTY_THINKING_CONFIG));
    }

    public void testOverrideWith_UnifiedCompletionRequest_UsesModelFields_WhenRequestDoesNotOverride() {
        var model = createCompletionModel(
            DEFAULT_PROJECT_ID,
            DEFAULT_LOCATION,
            DEFAULT_MODEL_ID,
            DEFAULT_API_KEY,
            DEFAULT_RATE_LIMIT,
            EMPTY_THINKING_CONFIG,
            null,
            null,
            123
        );
        var request = new UnifiedCompletionRequest(
            List.of(new UnifiedCompletionRequest.Message(new UnifiedCompletionRequest.ContentString("hello"), "user", null, null)),
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );

        var overriddenModel = GoogleVertexAiChatCompletionModel.of(model, request);

        assertThat(overriddenModel.getServiceSettings().modelId(), is(DEFAULT_MODEL_ID));

        assertThat(overriddenModel.getServiceSettings().projectId(), is(DEFAULT_PROJECT_ID));
        assertThat(overriddenModel.getServiceSettings().location(), is(DEFAULT_LOCATION));
        assertThat(overriddenModel.getServiceSettings().rateLimitSettings(), is(DEFAULT_RATE_LIMIT));
        assertThat(overriddenModel.getSecretSettings().serviceAccountJson(), equalTo(new SecureString(DEFAULT_API_KEY.toCharArray())));
        assertThat(overriddenModel.getTaskSettings().thinkingConfig(), is(EMPTY_THINKING_CONFIG));

        assertThat(overriddenModel, not(sameInstance(model)));
    }

    public void testBuildUri() throws URISyntaxException {
        String location = "us-east1";
        String projectId = "my-gcp-project";
        String model = "gemini-1.5-flash-001";
        URI expectedUri = new URI(
            "https://us-east1-aiplatform.googleapis.com/v1/projects/my-gcp-project"
                + "/locations/global/publishers/google/models/gemini-1.5-flash-001:streamGenerateContent?alt=sse"
        );
        URI actualUri = GoogleVertexAiChatCompletionModel.buildUriStreaming(location, projectId, model);
        assertThat(actualUri, is(expectedUri));
    }

    public void testOf_overridesTaskSettings_whenPresent() {
        var model = createCompletionModel(
            DEFAULT_PROJECT_ID,
            DEFAULT_LOCATION,
            DEFAULT_MODEL_ID,
            DEFAULT_API_KEY,
            DEFAULT_RATE_LIMIT,
            new ThinkingConfig(123),
            null,
            null,
            123
        );
        int newThinkingBudget = 456;
        Map<String, Object> taskSettings = new HashMap<>(
            Map.of(THINKING_CONFIG_FIELD, new HashMap<>(Map.of(THINKING_BUDGET_FIELD, newThinkingBudget)), "max_tokens", 456)
        );
        var overriddenModel = GoogleVertexAiChatCompletionModel.of(model, taskSettings);

        assertThat(overriddenModel.getServiceSettings().modelId(), is(DEFAULT_MODEL_ID));
        assertThat(overriddenModel.getServiceSettings().projectId(), is(DEFAULT_PROJECT_ID));
        assertThat(overriddenModel.getServiceSettings().location(), is(DEFAULT_LOCATION));
        assertThat(overriddenModel.getServiceSettings().rateLimitSettings(), is(DEFAULT_RATE_LIMIT));
        assertThat(overriddenModel.getSecretSettings().serviceAccountJson(), equalTo(new SecureString(DEFAULT_API_KEY.toCharArray())));

        assertThat(overriddenModel.getTaskSettings().thinkingConfig(), is(new ThinkingConfig(newThinkingBudget)));
        assertThat(overriddenModel.getTaskSettings().maxTokens(), is(456));
    }

    public void testOf_doesNotOverrideTaskSettings_whenNotPresent() {
        ThinkingConfig originalThinkingConfig = new ThinkingConfig(123);
        var model = createCompletionModel(
            DEFAULT_PROJECT_ID,
            DEFAULT_LOCATION,
            DEFAULT_MODEL_ID,
            DEFAULT_API_KEY,
            DEFAULT_RATE_LIMIT,
            originalThinkingConfig,
            null,
            null,
            123
        );
        Map<String, Object> taskSettings = new HashMap<>(Map.of(THINKING_CONFIG_FIELD, new HashMap<>()));
        var overriddenModel = GoogleVertexAiChatCompletionModel.of(model, taskSettings);

        assertThat(overriddenModel.getServiceSettings().modelId(), is(DEFAULT_MODEL_ID));
        assertThat(overriddenModel.getServiceSettings().projectId(), is(DEFAULT_PROJECT_ID));
        assertThat(overriddenModel.getServiceSettings().location(), is(DEFAULT_LOCATION));
        assertThat(overriddenModel.getServiceSettings().rateLimitSettings(), is(DEFAULT_RATE_LIMIT));
        assertThat(overriddenModel.getSecretSettings().serviceAccountJson(), equalTo(new SecureString(DEFAULT_API_KEY.toCharArray())));

        assertThat(overriddenModel.getTaskSettings().thinkingConfig(), is(originalThinkingConfig));
        assertThat(overriddenModel.getTaskSettings().maxTokens(), is(123));
    }

    public void testModelCreationForAnthropicBothUrls() throws URISyntaxException {
        var uri = new URI("http://example.com");
        var streamingUri = new URI("http://example-streaming.com");
        testModelCreation(uri, streamingUri, uri, streamingUri, GoogleModelGardenProvider.ANTHROPIC);
    }

    public void testModelCreationForAnthropicOnlyNonStreamingUrl() throws URISyntaxException {
        var uri = new URI("http://example.com");
        testModelCreation(uri, null, uri, uri, GoogleModelGardenProvider.ANTHROPIC);
    }

    public void testModelCreationForAnthropicOnlyStreamingUrl() throws URISyntaxException {
        var streamingUri = new URI("http://example-streaming.com");
        testModelCreation(null, streamingUri, streamingUri, streamingUri, GoogleModelGardenProvider.ANTHROPIC);
    }

    public void testModelCreationForMetaBothUrls() throws URISyntaxException {
        var uri = new URI("http://example.com");
        var streamingUri = new URI("http://example-streaming.com");
        testModelCreation(uri, streamingUri, uri, streamingUri, GoogleModelGardenProvider.META);
    }

    public void testModelCreationForMetaOnlyNonStreamingUrl() throws URISyntaxException {
        var uri = new URI("http://example.com");
        testModelCreation(uri, null, uri, uri, GoogleModelGardenProvider.META);
    }

    public void testModelCreationForMetaOnlyStreamingUrl() throws URISyntaxException {
        var streamingUri = new URI("http://example-streaming.com");
        testModelCreation(null, streamingUri, streamingUri, streamingUri, GoogleModelGardenProvider.META);
    }

    private static void testModelCreation(
        URI uri,
        URI streamingUri,
        URI expectedNonStreamingUri,
        URI expectedStreamingUri,
        GoogleModelGardenProvider provider
    ) {
        var model = createGoogleModelGardenChatCompletionModel(
            DEFAULT_API_KEY,
            DEFAULT_RATE_LIMIT,
            EMPTY_THINKING_CONFIG,
            provider,
            uri,
            streamingUri,
            123
        );
        var request = new UnifiedCompletionRequest(
            List.of(new UnifiedCompletionRequest.Message(new UnifiedCompletionRequest.ContentString("hello"), "user", null, null)),
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );

        var overriddenModel = GoogleVertexAiChatCompletionModel.of(model, request);

        assertNull(overriddenModel.getServiceSettings().modelId());
        assertThat(overriddenModel, not(sameInstance(model)));
        assertNull(overriddenModel.getServiceSettings().projectId());
        assertNull(overriddenModel.getServiceSettings().location());
        assertThat(overriddenModel.getServiceSettings().rateLimitSettings(), is(DEFAULT_RATE_LIMIT));
        assertThat(overriddenModel.getServiceSettings().uri(), is(uri));
        assertThat(overriddenModel.getServiceSettings().streamingUri(), is(streamingUri));
        assertThat(overriddenModel.getServiceSettings().provider(), is(provider));
        assertThat(overriddenModel.getSecretSettings().serviceAccountJson(), equalTo(new SecureString(DEFAULT_API_KEY.toCharArray())));
        assertThat(overriddenModel.getTaskSettings().thinkingConfig(), is(EMPTY_THINKING_CONFIG));
        assertThat(overriddenModel.getTaskSettings().maxTokens(), is(123));
        assertThat(overriddenModel.nonStreamingUri(), is(expectedNonStreamingUri));
        assertThat(overriddenModel.streamingURI(), is(expectedStreamingUri));
    }

    public static GoogleVertexAiChatCompletionModel createCompletionModel(
        String projectId,
        String location,
        String modelId,
        String apiKey,
        RateLimitSettings rateLimitSettings,
        ThinkingConfig thinkingConfig,
        GoogleModelGardenProvider provider,
        URI uri,
        Integer maxTokens
    ) {
        return new GoogleVertexAiChatCompletionModel(
            "google-vertex-ai-chat-test-id",
            TaskType.CHAT_COMPLETION,
            "google_vertex_ai",
            new GoogleVertexAiChatCompletionServiceSettings(projectId, location, modelId, uri, uri, provider, rateLimitSettings),
            new GoogleVertexAiChatCompletionTaskSettings(thinkingConfig, maxTokens),
            new GoogleVertexAiSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public static GoogleVertexAiChatCompletionModel createGoogleModelGardenChatCompletionModel(
        String apiKey,
        RateLimitSettings rateLimitSettings,
        ThinkingConfig thinkingConfig,
        GoogleModelGardenProvider provider,
        URI uri,
        URI streamingUri,
        int maxTokens
    ) {
        return new GoogleVertexAiChatCompletionModel(
            "google-vertex-ai-chat-test-id",
            TaskType.CHAT_COMPLETION,
            "google_vertex_ai",
            new GoogleVertexAiChatCompletionServiceSettings(null, null, null, uri, streamingUri, provider, rateLimitSettings),
            new GoogleVertexAiChatCompletionTaskSettings(thinkingConfig, maxTokens),
            new GoogleVertexAiSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public static URI buildDefaultUri() throws URISyntaxException {
        return GoogleVertexAiChatCompletionModel.buildUriStreaming(DEFAULT_LOCATION, DEFAULT_PROJECT_ID, DEFAULT_MODEL_ID);
    }
}
