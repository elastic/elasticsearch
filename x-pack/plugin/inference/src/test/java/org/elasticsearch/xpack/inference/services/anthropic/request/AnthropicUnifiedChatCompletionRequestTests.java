/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.anthropic.request;

import org.elasticsearch.action.support.TestPlainActionFuture;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.inference.completion.ContentString;
import org.elasticsearch.inference.completion.Message;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.services.anthropic.completion.AnthropicChatCompletionModelTests;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.services.anthropic.request.AnthropicRequestUtils.ANTHROPIC_VERSION_2023_06_01;
import static org.elasticsearch.xpack.inference.services.anthropic.request.AnthropicRequestUtils.VERSION;
import static org.elasticsearch.xpack.inference.services.anthropic.request.AnthropicRequestUtils.X_API_KEY;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class AnthropicUnifiedChatCompletionRequestTests extends ESTestCase {

    private static final String API_KEY = "test_api_key";
    private static final String MODEL_NAME = "claude-3-5-sonnet-latest";
    private static final String INPUT = "Hello, world!";
    private static final String ROLE = "user";

    public void testCreateHttpRequestSetsAnthropicHeaders() throws IOException, URISyntaxException {
        var maxTokens = 1024;
        var model = AnthropicChatCompletionModelTests.createChatCompletionModel(API_KEY, MODEL_NAME, maxTokens);
        var request = new AnthropicUnifiedChatCompletionRequest(unifiedChatInput(true), model);
        var httpRequest = createHttpRequest(request);

        var httpPost = httpRequest.httpRequest();
        assertThat(httpPost.getUri(), is(request.getURI()));
        assertThat(httpPost.getFirstHeader(X_API_KEY).getValue(), is(API_KEY));
        assertThat(httpPost.getFirstHeader(VERSION).getValue(), is(ANTHROPIC_VERSION_2023_06_01));
        assertThat(httpPost.getBody().getContentType().toString(), is("application/json; charset=UTF-8"));

        var requestMap = entityAsMap(httpPost.getBodyText());
        assertThat(requestMap, aMapWithSize(4));
        assertThat(requestMap.get("model"), is(MODEL_NAME));
        assertThat(requestMap.get("stream"), is(true));
        assertThat(requestMap.get("max_tokens"), is(maxTokens));
        assertThat(
            requestMap.get("messages"),
            is(List.of(Map.of("role", ROLE, "content", List.of(Map.of("type", "text", "text", INPUT)))))
        );
    }

    public void testCreateHttpRequestNonStreaming() throws IOException {
        var maxTokens = 512;
        var request = createRequest(false, maxTokens);
        var httpRequest = createHttpRequest(request);

        var httpPost = httpRequest.httpRequest();
        var requestMap = entityAsMap(httpPost.getBodyText());
        assertThat(requestMap.get("stream"), is(false));
        assertThat(requestMap.get("max_tokens"), is(maxTokens));
    }

    public void testTaskTypeReflectsModel() {
        var model = AnthropicChatCompletionModelTests.createChatCompletionModel(API_KEY, MODEL_NAME, 1024);
        var input = unifiedChatInput(true);
        var request = new AnthropicUnifiedChatCompletionRequest(input, model);

        assertThat(request.getTaskType(), equalTo(model.getTaskType()));
    }

    public void testTruncateReturnsSelf() {
        var request = createRequest(true, 1024);
        assertThat(request.truncate(), sameInstance(request));
        assertThat(request.getTruncationInfo(), equalTo(null));
    }

    public void testIsStreamingFromInput() {
        assertThat(createRequest(true, 1024).isStreaming(), is(true));
        assertThat(createRequest(false, 1024).isStreaming(), is(false));
    }

    private static AnthropicUnifiedChatCompletionRequest createRequest(boolean stream, int maxTokens) {
        var model = AnthropicChatCompletionModelTests.createChatCompletionModel(API_KEY, MODEL_NAME, maxTokens);
        return new AnthropicUnifiedChatCompletionRequest(unifiedChatInput(stream), model);
    }

    private static UnifiedChatInput unifiedChatInput(boolean stream) {
        var message = new Message(new ContentString(INPUT), ROLE, null, null);
        var unifiedRequest = UnifiedCompletionRequest.of(List.of(message));
        return new UnifiedChatInput(unifiedRequest, stream);
    }

    private static HttpRequest createHttpRequest(AnthropicUnifiedChatCompletionRequest request) {
        var future = new TestPlainActionFuture<HttpRequest>();
        request.createHttpRequest(future);
        return future.actionGet();
    }
}
