/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.request.completion;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleModelGardenProvider;
import org.elasticsearch.xpack.inference.services.googlevertexai.completion.GoogleVertexAiChatCompletionModel;
import org.elasticsearch.xpack.inference.services.googlevertexai.completion.GoogleVertexAiChatCompletionModelTests;
import org.elasticsearch.xpack.inference.services.googlevertexai.completion.ThinkingConfig;
import org.elasticsearch.xpack.inference.services.googlevertexai.request.GoogleVertexAiRequest;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class GoogleVertexAiUnifiedChatCompletionRequestTests extends ESTestCase {

    private static final String AUTH_HEADER_VALUE = "Bearer foo";

    public void testCreateRequest_Default() throws IOException {
        var requestMap = testCreateRequest(null);
        assertThat(requestMap, aMapWithSize(1));
        assertThat(
            requestMap,
            equalTo(Map.of("contents", List.of(Map.of("role", "user", "parts", List.of(Map.of("text", "Hello Gemini!"))))))
        );

    }

    public void testCreateRequest_Anthropic() throws IOException {
        var requestMap = testCreateRequest(GoogleModelGardenProvider.ANTHROPIC);
        assertThat(requestMap, aMapWithSize(4));
        assertThat(
            requestMap,
            equalTo(
                Map.of(
                    "stream",
                    true,
                    "max_tokens",
                    1024,
                    "messages",
                    List.of(Map.of("role", "user", "content", "Hello Gemini!")),
                    "anthropic_version",
                    "vertex-2023-10-16"
                )
            )
        );

    }

    private static Map<String, Object> testCreateRequest(GoogleModelGardenProvider googleModelGardenProvider) throws IOException {
        var modelId = "gemini-pro";
        var projectId = "test-project";
        var location = "us-central1";

        var messages = List.of("Hello Gemini!");

        var request = createRequest(projectId, location, modelId, messages, null, null, null, googleModelGardenProvider);
        var httpRequest = request.createHttpRequest();
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        var uri = URI.create(
            Strings.format(
                "https://%s-aiplatform.googleapis.com/v1/projects/%s/locations/global/publishers"
                    + "/google/models/%s:streamGenerateContent?alt=sse",
                location,
                projectId,
                modelId
            )
        );

        assertThat(httpPost.getURI(), equalTo(uri));
        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is(AUTH_HEADER_VALUE));

        return entityAsMap(httpPost.getEntity().getContent());
    }

    public static GoogleVertexAiUnifiedChatCompletionRequest createRequest(
        String projectId,
        String location,
        String modelId,
        List<String> messages,
        @Nullable String apiKey,
        @Nullable RateLimitSettings rateLimitSettings,
        @Nullable ThinkingConfig thinkingConfig,
        @Nullable GoogleModelGardenProvider provider
    ) {
        var model = GoogleVertexAiChatCompletionModelTests.createCompletionModel(
            projectId,
            location,
            modelId,
            Objects.requireNonNullElse(apiKey, "default-api-key"),
            Objects.requireNonNullElse(rateLimitSettings, new RateLimitSettings(100)),
            thinkingConfig,
            provider,
            null,
            null
        );
        var unifiedChatInput = new UnifiedChatInput(messages, "user", true);

        return new GoogleVertexAiUnifiedChatCompletionWithoutAuthRequest(unifiedChatInput, model);
    }

    /**
     * We use this class to fake the auth implementation to avoid static mocking of {@link GoogleVertexAiRequest}
     */
    private static class GoogleVertexAiUnifiedChatCompletionWithoutAuthRequest extends GoogleVertexAiUnifiedChatCompletionRequest {
        GoogleVertexAiUnifiedChatCompletionWithoutAuthRequest(UnifiedChatInput unifiedChatInput, GoogleVertexAiChatCompletionModel model) {
            super(unifiedChatInput, model);
        }

        @Override
        public void decorateWithAuth(HttpPost httpPost) {
            httpPost.setHeader(HttpHeaders.AUTHORIZATION, AUTH_HEADER_VALUE);
        }
    }
}
