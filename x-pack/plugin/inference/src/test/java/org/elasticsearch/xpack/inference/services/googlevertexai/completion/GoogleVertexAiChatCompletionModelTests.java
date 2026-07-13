/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.completion;

import org.apache.http.HttpHeaders;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.inference.completion.ContentString;
import org.elasticsearch.inference.completion.Message;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleModelGardenProvider;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiServiceFields.MAX_TOKENS;
import static org.elasticsearch.xpack.inference.services.googlevertexai.completion.ThinkingConfig.THINKING_BUDGET_FIELD;
import static org.elasticsearch.xpack.inference.services.googlevertexai.completion.ThinkingConfig.THINKING_CONFIG_FIELD;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class GoogleVertexAiChatCompletionModelTests extends ESTestCase {

    private static final String TEST_PROJECT_ID = "test-project";
    private static final String TEST_LOCATION = "us-central1";
    private static final String TEST_MODEL_ID = "gemini-pro";
    private static final String TEST_API_KEY = "test-api-key";
    private static final RateLimitSettings TEST_RATE_LIMIT = new RateLimitSettings(100);
    private static final ThinkingConfig EMPTY_THINKING_CONFIG = new ThinkingConfig();
    private static final int TEST_MAX_TOKENS = 123;
    private static final int TEST_THINKING_BUDGET = 456;
    private static final String TEST_INFERENCE_ID = "google-vertex-ai-chat-test-id";
    private static final String TEST_SERVICE_NAME = "google_vertex_ai";
    private static final String TEST_STREAMING_URL = "http://example-streaming.com";
    private static final String TEST_NON_STREAMING_URL = "http://example.com";

    public void testOverrideWith_UnifiedCompletionRequest_OverridesModelId() {
        var model = createCompletionModel(
            TEST_PROJECT_ID,
            TEST_LOCATION,
            TEST_MODEL_ID,
            TEST_API_KEY,
            TEST_RATE_LIMIT,
            EMPTY_THINKING_CONFIG,
            null,
            null,
            null
        );
        var requestModelId = "gemini-flash";
        var request = new UnifiedCompletionRequest(
            List.of(new Message(new ContentString("hello"), "user", null, null)),
            requestModelId,
            null,
            null,
            null,
            null,
            null,
            null
        );

        var overriddenModel = GoogleVertexAiChatCompletionModel.of(model, request);

        assertThat(overriddenModel.getServiceSettings().modelId(), is(requestModelId));

        assertThat(overriddenModel, not(sameInstance(model)));
        assertThat(overriddenModel.getServiceSettings().projectId(), is(TEST_PROJECT_ID));
        assertThat(overriddenModel.getServiceSettings().location(), is(TEST_LOCATION));
        assertThat(overriddenModel.getServiceSettings().rateLimitSettings(), is(TEST_RATE_LIMIT));
        assertThat(overriddenModel.getSecretSettings().serviceAccountJson(), equalTo(new SecureString(TEST_API_KEY.toCharArray())));
        assertThat(overriddenModel.getTaskSettings().thinkingConfig(), is(EMPTY_THINKING_CONFIG));
    }

    public void testOverrideWith_UnifiedCompletionRequest_UsesModelFields_WhenRequestDoesNotOverride() {
        var model = createCompletionModel(
            TEST_PROJECT_ID,
            TEST_LOCATION,
            TEST_MODEL_ID,
            TEST_API_KEY,
            TEST_RATE_LIMIT,
            EMPTY_THINKING_CONFIG,
            null,
            null,
            TEST_MAX_TOKENS
        );
        var request = new UnifiedCompletionRequest(
            List.of(new Message(new ContentString("hello"), "user", null, null)),
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );

        var overriddenModel = GoogleVertexAiChatCompletionModel.of(model, request);

        assertThat(overriddenModel.getServiceSettings().modelId(), is(TEST_MODEL_ID));

        assertThat(overriddenModel.getServiceSettings().projectId(), is(TEST_PROJECT_ID));
        assertThat(overriddenModel.getServiceSettings().location(), is(TEST_LOCATION));
        assertThat(overriddenModel.getServiceSettings().rateLimitSettings(), is(TEST_RATE_LIMIT));
        assertThat(overriddenModel.getSecretSettings().serviceAccountJson(), equalTo(new SecureString(TEST_API_KEY.toCharArray())));
        assertThat(overriddenModel.getTaskSettings().thinkingConfig(), is(EMPTY_THINKING_CONFIG));

        assertThat(overriddenModel, not(sameInstance(model)));
    }

    public void testBuildStreamingUri() throws URISyntaxException {
        var expectedUri = new URI(Strings.format("""
            https://%s-aiplatform.googleapis.com/v1/projects/%s\
            /locations/global/publishers/google/models/%s:streamGenerateContent?alt=sse""", TEST_LOCATION, TEST_PROJECT_ID, TEST_MODEL_ID));
        URI actualUri = GoogleVertexAiChatCompletionModel.buildStreamingUri(TEST_LOCATION, TEST_PROJECT_ID, TEST_MODEL_ID);
        assertThat(actualUri, is(expectedUri));
    }

    public void testBuildNonStreamingUri() throws URISyntaxException {
        URI expectedUri = new URI(Strings.format("""
            https://%s-aiplatform.googleapis.com/v1/projects/%s\
            /locations/global/publishers/google/models/%s:generateContent""", TEST_LOCATION, TEST_PROJECT_ID, TEST_MODEL_ID));
        URI actualUri = GoogleVertexAiChatCompletionModel.buildNonStreamingUri(TEST_LOCATION, TEST_PROJECT_ID, TEST_MODEL_ID);
        assertThat(actualUri, is(expectedUri));
    }

    public void testBuildStreamingUri_NullLocation_UsesGlobalHost() throws URISyntaxException {
        assertBuildStreamingUri_UsesGlobalHost(null);
    }

    public void testBuildStreamingUri_EmptyLocation_UsesGlobalHost() throws URISyntaxException {
        assertBuildStreamingUri_UsesGlobalHost("");
    }

    private static void assertBuildStreamingUri_UsesGlobalHost(String location) throws URISyntaxException {
        URI expectedStreamingUri = new URI(Strings.format("""
            https://aiplatform.googleapis.com/v1/projects/%s\
            /locations/global/publishers/google/models/%s:streamGenerateContent?alt=sse""", TEST_PROJECT_ID, TEST_MODEL_ID));
        assertThat(GoogleVertexAiChatCompletionModel.buildStreamingUri(location, TEST_PROJECT_ID, TEST_MODEL_ID), is(expectedStreamingUri));
    }

    public void testBuildNonStreamingUri_EmptyLocation_UsesGlobalHost() throws URISyntaxException {
        assertBuildNonStreamingUri_UsesGlobalHost("");
    }

    public void testBuildNonStreamingUri_NullLocation_UsesGlobalHost() throws URISyntaxException {
        assertBuildNonStreamingUri_UsesGlobalHost(null);
    }

    private static void assertBuildNonStreamingUri_UsesGlobalHost(String location) throws URISyntaxException {
        URI expectedNonStreamingUri = new URI(Strings.format("""
            https://aiplatform.googleapis.com/v1/projects/%s\
            /locations/global/publishers/google/models/%s:generateContent""", TEST_PROJECT_ID, TEST_MODEL_ID));
        assertThat(
            GoogleVertexAiChatCompletionModel.buildNonStreamingUri(location, TEST_PROJECT_ID, TEST_MODEL_ID),
            is(expectedNonStreamingUri)
        );
    }

    public void testOf_overridesTaskSettings_whenPresent() {
        var model = createCompletionModel(
            TEST_PROJECT_ID,
            TEST_LOCATION,
            TEST_MODEL_ID,
            TEST_API_KEY,
            TEST_RATE_LIMIT,
            new ThinkingConfig(TEST_THINKING_BUDGET),
            null,
            null,
            TEST_MAX_TOKENS
        );
        int newThinkingBudget = 654;
        int newMaxTokens = 321;
        Map<String, Object> taskSettings = new HashMap<>(
            Map.of(THINKING_CONFIG_FIELD, new HashMap<>(Map.of(THINKING_BUDGET_FIELD, newThinkingBudget)), MAX_TOKENS, newMaxTokens)
        );
        var overriddenModel = GoogleVertexAiChatCompletionModel.of(model, taskSettings);

        assertThat(overriddenModel.getServiceSettings().modelId(), is(TEST_MODEL_ID));
        assertThat(overriddenModel.getServiceSettings().projectId(), is(TEST_PROJECT_ID));
        assertThat(overriddenModel.getServiceSettings().location(), is(TEST_LOCATION));
        assertThat(overriddenModel.getServiceSettings().rateLimitSettings(), is(TEST_RATE_LIMIT));
        assertThat(overriddenModel.getSecretSettings().serviceAccountJson(), equalTo(new SecureString(TEST_API_KEY.toCharArray())));

        assertThat(overriddenModel.getTaskSettings().thinkingConfig(), is(new ThinkingConfig(newThinkingBudget)));
        assertThat(overriddenModel.getTaskSettings().maxTokens(), is(newMaxTokens));
    }

    public void testOf_doesNotOverrideTaskSettings_whenNotPresent() {
        ThinkingConfig originalThinkingConfig = new ThinkingConfig(TEST_THINKING_BUDGET);
        var model = createCompletionModel(
            TEST_PROJECT_ID,
            TEST_LOCATION,
            TEST_MODEL_ID,
            TEST_API_KEY,
            TEST_RATE_LIMIT,
            originalThinkingConfig,
            null,
            null,
            TEST_MAX_TOKENS
        );
        Map<String, Object> taskSettings = new HashMap<>(Map.of(THINKING_CONFIG_FIELD, new HashMap<>()));
        var overriddenModel = GoogleVertexAiChatCompletionModel.of(model, taskSettings);

        assertThat(overriddenModel.getServiceSettings().modelId(), is(TEST_MODEL_ID));
        assertThat(overriddenModel.getServiceSettings().projectId(), is(TEST_PROJECT_ID));
        assertThat(overriddenModel.getServiceSettings().location(), is(TEST_LOCATION));
        assertThat(overriddenModel.getServiceSettings().rateLimitSettings(), is(TEST_RATE_LIMIT));
        assertThat(overriddenModel.getSecretSettings().serviceAccountJson(), equalTo(new SecureString(TEST_API_KEY.toCharArray())));

        assertThat(overriddenModel.getTaskSettings().thinkingConfig(), is(originalThinkingConfig));
        assertThat(overriddenModel.getTaskSettings().maxTokens(), is(TEST_MAX_TOKENS));
    }

    public void testModelCreationForAnthropicBothUrls() throws URISyntaxException {
        var nonStreamingUri = new URI(TEST_NON_STREAMING_URL);
        var streamingUri = new URI(TEST_STREAMING_URL);
        testModelCreation(nonStreamingUri, streamingUri, nonStreamingUri, streamingUri, GoogleModelGardenProvider.ANTHROPIC);
    }

    public void testModelCreationForAnthropicOnlyNonStreamingUrl() throws URISyntaxException {
        var nonStreamingUri = new URI(TEST_NON_STREAMING_URL);
        testModelCreation(nonStreamingUri, null, nonStreamingUri, nonStreamingUri, GoogleModelGardenProvider.ANTHROPIC);
    }

    public void testModelCreationForAnthropicOnlyStreamingUrl() throws URISyntaxException {
        var streamingUri = new URI(TEST_STREAMING_URL);
        testModelCreation(null, streamingUri, streamingUri, streamingUri, GoogleModelGardenProvider.ANTHROPIC);
    }

    public void testModelCreationForMetaBothUrls() throws URISyntaxException {
        var nonStreamingUri = new URI(TEST_NON_STREAMING_URL);
        var streamingUri = new URI(TEST_STREAMING_URL);
        testModelCreation(nonStreamingUri, streamingUri, nonStreamingUri, streamingUri, GoogleModelGardenProvider.META);
    }

    public void testModelCreationForMetaOnlyNonStreamingUrl() throws URISyntaxException {
        var nonStreamingUri = new URI(TEST_NON_STREAMING_URL);
        testModelCreation(nonStreamingUri, null, nonStreamingUri, nonStreamingUri, GoogleModelGardenProvider.META);
    }

    public void testModelCreationForMetaOnlyStreamingUrl() throws URISyntaxException {
        var streamingUri = new URI(TEST_STREAMING_URL);
        testModelCreation(null, streamingUri, streamingUri, streamingUri, GoogleModelGardenProvider.META);
    }

    private static void testModelCreation(
        URI nonStreamingUri,
        URI streamingUri,
        URI expectedNonStreamingUri,
        URI expectedStreamingUri,
        GoogleModelGardenProvider provider
    ) {
        var model = createGoogleModelGardenChatCompletionModel(
            TEST_API_KEY,
            TEST_RATE_LIMIT,
            EMPTY_THINKING_CONFIG,
            provider,
            nonStreamingUri,
            streamingUri,
            TEST_MAX_TOKENS
        );
        var request = new UnifiedCompletionRequest(
            List.of(new Message(new ContentString("hello"), "user", null, null)),
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
        assertThat(overriddenModel.getServiceSettings().rateLimitSettings(), is(TEST_RATE_LIMIT));
        assertThat(overriddenModel.getServiceSettings().uri(), is(nonStreamingUri));
        assertThat(overriddenModel.getServiceSettings().streamingUri(), is(streamingUri));
        assertThat(overriddenModel.getServiceSettings().provider(), is(provider));
        assertThat(overriddenModel.getSecretSettings().serviceAccountJson(), equalTo(new SecureString(TEST_API_KEY.toCharArray())));
        assertThat(overriddenModel.getTaskSettings().thinkingConfig(), is(EMPTY_THINKING_CONFIG));
        assertThat(overriddenModel.getTaskSettings().maxTokens(), is(TEST_MAX_TOKENS));
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
            TEST_INFERENCE_ID,
            TaskType.CHAT_COMPLETION,
            TEST_SERVICE_NAME,
            new GoogleVertexAiChatCompletionServiceSettings(projectId, location, modelId, uri, uri, provider, rateLimitSettings),
            new GoogleVertexAiChatCompletionTaskSettings(thinkingConfig, maxTokens),
            new GoogleVertexAiSecretSettings(new SecureString(apiKey.toCharArray()))
        );
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
        Integer maxTokens,
        String authHeaderValue
    ) {
        return new GoogleVertexAiChatCompletionModel(
            TEST_INFERENCE_ID,
            TaskType.CHAT_COMPLETION,
            TEST_SERVICE_NAME,
            new GoogleVertexAiChatCompletionServiceSettings(projectId, location, modelId, uri, uri, provider, rateLimitSettings),
            new GoogleVertexAiChatCompletionTaskSettings(thinkingConfig, maxTokens),
            new GoogleVertexAiSecretSettings(new SecureString(apiKey.toCharArray())),
            (httpPost, model) -> httpPost.setHeader(HttpHeaders.AUTHORIZATION, authHeaderValue)
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
            TEST_INFERENCE_ID,
            TaskType.CHAT_COMPLETION,
            TEST_SERVICE_NAME,
            new GoogleVertexAiChatCompletionServiceSettings(null, null, null, uri, streamingUri, provider, rateLimitSettings),
            new GoogleVertexAiChatCompletionTaskSettings(thinkingConfig, maxTokens),
            new GoogleVertexAiSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public static URI buildDefaultUri() throws URISyntaxException {
        return GoogleVertexAiChatCompletionModel.buildStreamingUri(TEST_LOCATION, TEST_PROJECT_ID, TEST_MODEL_ID);
    }
}
