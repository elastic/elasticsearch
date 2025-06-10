/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.completion;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

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

    public void testOverrideWith_UnifiedCompletionRequest_OverridesModelId() {
        var model = createCompletionModel(DEFAULT_PROJECT_ID, DEFAULT_LOCATION, DEFAULT_MODEL_ID, DEFAULT_API_KEY, DEFAULT_RATE_LIMIT);
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
        assertThat(overriddenModel.getTaskSettings(), is(model.getTaskSettings()));
    }

    public void testOverrideWith_UnifiedCompletionRequest_UsesModelFields_WhenRequestDoesNotOverride() {
        var model = createCompletionModel(DEFAULT_PROJECT_ID, DEFAULT_LOCATION, DEFAULT_MODEL_ID, DEFAULT_API_KEY, DEFAULT_RATE_LIMIT);
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
        assertThat(overriddenModel.getTaskSettings(), is(model.getTaskSettings()));

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

    public static GoogleVertexAiChatCompletionModel createCompletionModel(
        String projectId,
        String location,
        String modelId,
        String apiKey,
        RateLimitSettings rateLimitSettings
    ) {
        return new GoogleVertexAiChatCompletionModel(
            "google-vertex-ai-chat-test-id",
            TaskType.CHAT_COMPLETION,
            "google_vertex_ai",
            new GoogleVertexAiChatCompletionServiceSettings(projectId, location, modelId, rateLimitSettings),
            new EmptyTaskSettings(),
            new GoogleVertexAiSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public static URI buildDefaultUri() throws URISyntaxException {
        return GoogleVertexAiChatCompletionModel.buildUriStreaming(DEFAULT_LOCATION, DEFAULT_PROJECT_ID, DEFAULT_MODEL_ID);
    }
}
