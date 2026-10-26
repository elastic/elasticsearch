/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.anthropic.request;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.request.RequestTests;
import org.elasticsearch.xpack.inference.services.anthropic.completion.AnthropicChatCompletionModelTests;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.is;

public class AnthropicChatCompletionRequestTests extends ESTestCase {

    public void testCreateRequest() throws IOException, URISyntaxException {
        var request = createRequest("secret", "abc", "model", 2);
        var httpRequest = RequestTests.getHttpRequestSync(request);

        var httpPost = httpRequest.httpRequest();

        assertThat(httpPost.getUri().toString(), is(buildAnthropicUri()));
        assertThat(httpPost.getBody().getContentType().toString(), is("application/json; charset=UTF-8"));
        assertThat(httpPost.getLastHeader(AnthropicRequestUtils.X_API_KEY).getValue(), is("secret"));
        assertThat(
            httpPost.getLastHeader(AnthropicRequestUtils.VERSION).getValue(),
            is(AnthropicRequestUtils.ANTHROPIC_VERSION_2023_06_01)
        );

        var requestMap = entityAsMap(httpPost.getBodyText());
        assertThat(requestMap, aMapWithSize(3));
        assertThat(requestMap.get("messages"), is(List.of(Map.of("role", "user", "content", "abc"))));
        assertThat(requestMap.get("model"), is("model"));
        assertThat(requestMap.get("max_tokens"), is(2));
    }

    public void testCreateRequest_TestUrl() throws IOException, URISyntaxException {
        var request = createRequest("fake_url", "secret", "abc", "model", 2);
        var httpRequest = RequestTests.getHttpRequestSync(request);

        var httpPost = httpRequest.httpRequest();

        assertThat(httpPost.getUri().toString(), is("fake_url"));
        assertThat(httpPost.getBody().getContentType().toString(), is("application/json; charset=UTF-8"));
        assertThat(httpPost.getLastHeader(AnthropicRequestUtils.X_API_KEY).getValue(), is("secret"));
        assertThat(
            httpPost.getLastHeader(AnthropicRequestUtils.VERSION).getValue(),
            is(AnthropicRequestUtils.ANTHROPIC_VERSION_2023_06_01)
        );

        var requestMap = entityAsMap(httpPost.getBodyText());
        assertThat(requestMap, aMapWithSize(3));
        assertThat(requestMap.get("messages"), is(List.of(Map.of("role", "user", "content", "abc"))));
        assertThat(requestMap.get("model"), is("model"));
        assertThat(requestMap.get("max_tokens"), is(2));
    }

    public void testTruncate_DoesNotReduceInputTextSize() throws IOException {
        var request = createRequest("secret", "abc", "model", 2);

        var truncatedRequest = request.truncate();
        assertThat(request.getURI().toString(), is(buildAnthropicUri()));

        var httpRequest = RequestTests.getHttpRequestSync(truncatedRequest);

        var httpPost = httpRequest.httpRequest();
        var requestMap = entityAsMap(httpPost.getBodyText());
        assertThat(requestMap, aMapWithSize(3));

        // We do not truncate for Anthropic chat completions
        assertThat(requestMap.get("messages"), is(List.of(Map.of("role", "user", "content", "abc"))));
        assertThat(requestMap.get("model"), is("model"));
        assertThat(requestMap.get("max_tokens"), is(2));
    }

    public void testTruncationInfo_ReturnsNull() {
        var request = createRequest("secret", "abc", "model", 2);
        assertNull(request.getTruncationInfo());
    }

    public static AnthropicChatCompletionRequest createRequest(String apiKey, String input, String model, int maxTokens) {
        var chatCompletionModel = AnthropicChatCompletionModelTests.createChatCompletionModel(apiKey, model, maxTokens);
        return new AnthropicChatCompletionRequest(List.of(input), chatCompletionModel, false);
    }

    public static AnthropicChatCompletionRequest createRequest(String url, String apiKey, String input, String model, int maxTokens) {
        var chatCompletionModel = AnthropicChatCompletionModelTests.createChatCompletionModel(url, apiKey, model, maxTokens);
        return new AnthropicChatCompletionRequest(List.of(input), chatCompletionModel, false);
    }

    private static String buildAnthropicUri() {
        return Strings.format(
            "https://%s/%s/%s",
            AnthropicRequestUtils.HOST,
            AnthropicRequestUtils.API_VERSION_1,
            AnthropicRequestUtils.MESSAGES_PATH
        );
    }
}
