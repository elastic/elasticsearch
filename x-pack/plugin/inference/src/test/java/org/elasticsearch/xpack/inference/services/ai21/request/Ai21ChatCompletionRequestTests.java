/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ai21.request;

import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.ai21.completion.Ai21ChatCompletionModelTests;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class Ai21ChatCompletionRequestTests extends ESTestCase {

    public void testCreateRequest_WithStreaming() throws IOException {
        var request = createRequest("secret", randomAlphaOfLength(15), "model", true);
        var httpRequest = request.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap.get("stream"), is(true));
    }

    public void testTruncate_DoesNotReduceInputTextSize() throws IOException {
        String input = randomAlphaOfLength(5);
        var request = createRequest("secret", input, "model", true);
        var truncatedRequest = request.truncate();
        assertThat(request.getURI().toString(), is("https://api.ai21.com/studio/v1/chat/completions"));

        var httpRequest = truncatedRequest.createHttpRequest();
        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();
        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(5));

        // We do not truncate for AI21 chat completions
        assertThat(requestMap.get("messages"), is(List.of(Map.of("role", "user", "content", input))));
        assertThat(requestMap.get("model"), is("model"));
        assertThat(requestMap.get("n"), is(1));
        assertTrue((Boolean) requestMap.get("stream"));
        assertThat(requestMap.get("stream_options"), is(Map.of("include_usage", true)));
    }

    public void testTruncationInfo_ReturnsNull() {
        var request = createRequest("secret", randomAlphaOfLength(5), "model", true);
        assertNull(request.getTruncationInfo());
    }

    public static Ai21ChatCompletionRequest createRequest(String apiKey, String input, @Nullable String model) {
        return createRequest(apiKey, input, model, false);
    }

    public static Ai21ChatCompletionRequest createRequest(String apiKey, String input, @Nullable String model, boolean stream) {
        var chatCompletionModel = Ai21ChatCompletionModelTests.createCompletionModel(apiKey, model);
        return new Ai21ChatCompletionRequest(new UnifiedChatInput(List.of(input), "user", stream), chatCompletionModel);
    }

}
