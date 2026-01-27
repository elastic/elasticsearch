/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.request;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.openai.completion.OpenAiChatCompletionModelTests;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.services.openai.OpenAiUtils.ORGANIZATION_HEADER;
import static org.elasticsearch.xpack.inference.services.openai.completion.OpenAiChatCompletionModel.buildDefaultUri;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class OpenAiUnifiedChatCompletionRequestTests extends ESTestCase {

    public void testCreateRequest_WithUrlOrganizationUserDefined() throws IOException {
        var request = createRequest("www.google.com", "org", "secret", "abc", "model", "user", true);
        var httpRequest = request.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getURI().toString(), is("www.google.com"));
        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer secret"));
        assertThat(httpPost.getLastHeader(ORGANIZATION_HEADER).getValue(), is("org"));

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertRequestMapWithUser(requestMap, "user");
    }

    private void assertRequestMapWithoutUser(Map<String, Object> requestMap) {
        assertRequestMapWithUser(requestMap, null);
    }

    private void assertRequestMapWithUser(Map<String, Object> requestMap, @Nullable String user) {
        assertThat(requestMap, aMapWithSize(user != null ? 6 : 5));
        assertThat(requestMap.get("messages"), is(List.of(Map.of("role", "user", "content", "abc"))));
        assertThat(requestMap.get("model"), is("model"));
        if (user != null) {
            assertThat(requestMap.get("user"), is(user));
        }
        assertThat(requestMap.get("n"), is(1));
        assertTrue((Boolean) requestMap.get("stream"));
        assertThat(requestMap.get("stream_options"), is(Map.of("include_usage", true)));
    }

    public void testCreateRequest_WithDefaultUrl() throws URISyntaxException, IOException {
        var request = createRequest(null, "org", "secret", "abc", "model", "user", true);
        var httpRequest = request.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getURI().toString(), is(buildDefaultUri().toString()));
        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer secret"));
        assertThat(httpPost.getLastHeader(ORGANIZATION_HEADER).getValue(), is("org"));

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertRequestMapWithUser(requestMap, "user");

    }

    public void testCreateRequest_WithDefaultUrlAndWithoutUserOrganization() throws URISyntaxException, IOException {
        var request = createRequest(null, null, "secret", "abc", "model", null, true);
        var httpRequest = request.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getURI().toString(), is(buildDefaultUri().toString()));
        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer secret"));
        assertNull(httpPost.getLastHeader(ORGANIZATION_HEADER));

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertRequestMapWithoutUser(requestMap);
    }

    public void testCreateRequest_WithStreaming() throws IOException {
        var request = createRequest(null, null, "secret", "abc", "model", null, true);
        var httpRequest = request.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap.get("stream"), is(true));
    }

    public void testTruncate_DoesNotReduceInputTextSize() throws URISyntaxException, IOException {
        var request = createRequest(null, null, "secret", "abcd", "model", null, true);
        var truncatedRequest = request.truncate();
        assertThat(request.getURI().toString(), is(buildDefaultUri().toString()));

        var httpRequest = truncatedRequest.createHttpRequest();
        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();
        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(5));

        // We do not truncate for OpenAi chat completions
        assertThat(requestMap.get("messages"), is(List.of(Map.of("role", "user", "content", "abcd"))));
        assertThat(requestMap.get("model"), is("model"));
        assertThat(requestMap.get("n"), is(1));
        assertTrue((Boolean) requestMap.get("stream"));
        assertThat(requestMap.get("stream_options"), is(Map.of("include_usage", true)));
    }

    public void testTruncationInfo_ReturnsNull() {
        var request = createRequest(null, null, "secret", "abcd", "model", null, true);
        assertNull(request.getTruncationInfo());
    }

    public static OpenAiUnifiedChatCompletionRequest createRequest(
        @Nullable String url,
        @Nullable String org,
        String apiKey,
        String input,
        String model,
        @Nullable String user
    ) {
        return createRequest(url, org, apiKey, input, model, user, false);
    }

    public static OpenAiUnifiedChatCompletionRequest createRequest(
        @Nullable String url,
        @Nullable String org,
        String apiKey,
        String input,
        String model,
        @Nullable String user,
        boolean stream
    ) {
        var chatCompletionModel = OpenAiChatCompletionModelTests.createCompletionModel(url, org, apiKey, model, user);
        return new OpenAiUnifiedChatCompletionRequest(new UnifiedChatInput(List.of(input), "user", stream), chatCompletionModel);
    }

}
