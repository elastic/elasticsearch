/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.groq.request;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.groq.GroqService;
import org.elasticsearch.xpack.inference.services.groq.GroqUtils;
import org.elasticsearch.xpack.inference.services.groq.completion.GroqChatCompletionModel;
import org.elasticsearch.xpack.inference.services.openai.OpenAiServiceFields;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.services.groq.completion.GroqChatCompletionModel.buildDefaultUri;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class GroqUnifiedChatCompletionRequestTests extends ESTestCase {

    private static final String ORG_HEADER = GroqUtils.createOrgHeader("org").getName();

    public void testCreateRequestWithUrlOrganizationAndHeaders() throws IOException {
        var request = createRequest("https://example.org/custom", "org-1", "api-key", "hello", "model", "user", true);
        var httpRequest = request.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getURI().toString(), is("https://example.org/custom"));
        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer api-key"));
        assertThat(httpPost.getLastHeader(ORG_HEADER).getValue(), is("org-1"));
        assertThat(httpPost.getLastHeader("X-Test").getValue(), is("1"));

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertRequestWithUser(requestMap, "user", "hello", true);
    }

    public void testCreateRequestUsesDefaultUrl() throws URISyntaxException, IOException {
        var request = createRequest(null, null, "api-key", "hello", "model", null, false);
        var httpRequest = request.createHttpRequest();
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getURI().toString(), is(buildDefaultUri().toString()));
        assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer api-key"));
        assertNull(httpPost.getLastHeader(ORG_HEADER));

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertRequestWithoutUser(requestMap, "hello", false);
    }

    public void testTruncateIsNoop() throws IOException {
        var request = createRequest(null, null, "api-key", "hello", "model", null, true);
        assertSame(request, request.truncate());
    }

    public void testTruncationInfoIsNull() {
        var request = createRequest(null, null, "api-key", "hello", "model", null, true);
        assertNull(request.getTruncationInfo());
    }

    private void assertRequestWithoutUser(Map<String, Object> requestMap, String expectedContent, boolean stream) {
        assertRequestWithUser(requestMap, null, expectedContent, stream);
    }

    private void assertRequestWithUser(Map<String, Object> requestMap, @Nullable String user, String expectedContent, boolean stream) {
        assertThat(requestMap.get("messages"), is(List.of(Map.of("role", "user", "content", expectedContent))));
        assertThat(requestMap.get("model"), is("model"));
        assertThat(requestMap.get("n"), is(1));
        assertThat(requestMap.get("stream"), is(stream));
        int expectedSize = 4;
        if (stream) {
            assertThat(requestMap.get("stream_options"), is(Map.of("include_usage", true)));
            expectedSize++;
        } else {
            assertNull(requestMap.get("stream_options"));
        }

        if (user == null) {
            assertThat(requestMap, aMapWithSize(expectedSize));
        } else {
            assertThat(requestMap.get("user"), is(user));
            assertThat(requestMap, aMapWithSize(expectedSize + 1));
        }
    }

    private static GroqUnifiedChatCompletionRequest createRequest(
        @Nullable String url,
        @Nullable String org,
        String apiKey,
        String input,
        String model,
        @Nullable String user,
        boolean stream
    ) {
        var groqModel = createModel(url, org, apiKey, model, user);
        return new GroqUnifiedChatCompletionRequest(new UnifiedChatInput(List.of(input), "user", stream), groqModel);
    }

    private static GroqChatCompletionModel createModel(
        @Nullable String url,
        @Nullable String org,
        String apiKey,
        String model,
        @Nullable String user
    ) {
        Map<String, Object> serviceSettings = new HashMap<>();
        serviceSettings.put(ServiceFields.MODEL_ID, model);
        if (url != null) {
            serviceSettings.put(ServiceFields.URL, url);
        }
        if (org != null) {
            serviceSettings.put(OpenAiServiceFields.ORGANIZATION, org);
        }

        Map<String, Object> taskSettings = new HashMap<>();
        if (user != null) {
            taskSettings.put(OpenAiServiceFields.USER, user);
        }
        taskSettings.put(OpenAiServiceFields.HEADERS, Map.of("X-Test", "1"));

        Map<String, Object> secretSettings = new HashMap<>();
        secretSettings.put(DefaultSecretSettings.API_KEY, apiKey);

        return new GroqChatCompletionModel(
            "inference-id",
            TaskType.CHAT_COMPLETION,
            GroqService.NAME,
            serviceSettings,
            taskSettings,
            secretSettings,
            ConfigurationParseContext.REQUEST
        );
    }
}
