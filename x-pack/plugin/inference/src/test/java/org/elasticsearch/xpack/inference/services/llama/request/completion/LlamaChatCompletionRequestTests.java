/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.llama.request.completion;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.llama.completion.LlamaChatCompletionModelTests;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class LlamaChatCompletionRequestTests extends ESTestCase {

    public void testCreateRequest_WithStreaming() throws IOException {
        String input = randomAlphaOfLength(15);
        var request = createRequest("model", "url", "secret", input, true);
        var httpRequest = request.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(request.getURI().toString(), is("url"));
        assertThat(requestMap.get("stream"), is(true));
        assertThat(requestMap.get("model"), is("model"));
        assertThat(requestMap.get("n"), is(1));
        assertNull(requestMap.get("stream_options"));
        assertThat(requestMap.get("messages"), is(List.of(Map.of("role", "user", "content", input))));
        assertThat(httpPost.getFirstHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer secret"));
    }

    public void testCreateRequest_NoStreaming_NoAuthorization() throws IOException {
        String input = randomAlphaOfLength(15);
        var request = createRequestWithNoAuth("model", "url", input, false);
        var httpRequest = request.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(request.getURI().toString(), is("url"));
        assertThat(requestMap.get("stream"), is(false));
        assertThat(requestMap.get("model"), is("model"));
        assertThat(requestMap.get("n"), is(1));
        assertNull(requestMap.get("stream_options"));
        assertThat(requestMap.get("messages"), is(List.of(Map.of("role", "user", "content", input))));
        assertNull(httpPost.getFirstHeader("Authorization"));
    }

    public void testTruncate_DoesNotReduceInputTextSize() {
        String input = randomAlphaOfLength(5);
        var request = createRequest("model", "url", "secret", input, true);
        assertThat(request.truncate(), is(request));
    }

    public void testTruncationInfo_ReturnsNull() {
        var request = createRequest("model", "url", "secret", randomAlphaOfLength(5), true);
        assertNull(request.getTruncationInfo());
    }

    public static LlamaChatCompletionRequest createRequest(String modelId, String url, String apiKey, String input, boolean stream) {
        var chatCompletionModel = LlamaChatCompletionModelTests.createChatCompletionModel(modelId, url, apiKey);
        return new LlamaChatCompletionRequest(new UnifiedChatInput(List.of(input), "user", stream), chatCompletionModel);
    }

    public static LlamaChatCompletionRequest createRequestWithNoAuth(String modelId, String url, String input, boolean stream) {
        var chatCompletionModel = LlamaChatCompletionModelTests.createChatCompletionModelNoAuth(modelId, url);
        return new LlamaChatCompletionRequest(new UnifiedChatInput(List.of(input), "user", stream), chatCompletionModel);
    }
}
