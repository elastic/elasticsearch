/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface.request;

import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.huggingface.completion.HuggingFaceChatCompletionModelTests;
import org.elasticsearch.xpack.inference.services.huggingface.request.completion.HuggingFaceUnifiedChatCompletionRequest;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class HuggingFaceUnifiedChatCompletionRequestTests extends ESTestCase {

    public void testCreateRequest_WithStreaming() throws IOException {
        var request = createRequest("url", "secret", "abcd", "model", true);
        var httpRequest = request.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap.get("stream"), is(true));
    }

    public void testTruncate_DoesNotReduceInputTextSize() throws URISyntaxException, IOException {
        var request = createRequest("url", "secret", "abcd", "model", true);
        var truncatedRequest = request.truncate();
        assertThat(request.getURI().toString(), is("url"));

        var httpRequest = truncatedRequest.createHttpRequest();
        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();
        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(5));

        // We do not truncate for Hugging Face chat completions
        assertThat(requestMap.get("messages"), is(List.of(Map.of("role", "user", "content", "abcd"))));
        assertThat(requestMap.get("model"), is("model"));
        assertThat(requestMap.get("n"), is(1));
        assertTrue((Boolean) requestMap.get("stream"));
        assertThat(requestMap.get("stream_options"), is(Map.of("include_usage", true)));
    }

    public void testTruncationInfo_ReturnsNull() {
        var request = createRequest("url", "secret", "abcd", "model", true);
        assertNull(request.getTruncationInfo());
    }

    public static HuggingFaceUnifiedChatCompletionRequest createRequest(String url, String apiKey, String input, @Nullable String model) {
        return createRequest(url, apiKey, input, model, false);
    }

    public static HuggingFaceUnifiedChatCompletionRequest createRequest(
        @Nullable String url,
        String apiKey,
        String input,
        @Nullable String model,
        boolean stream
    ) {
        var chatCompletionModel = HuggingFaceChatCompletionModelTests.createCompletionModel(url, apiKey, model);
        return new HuggingFaceUnifiedChatCompletionRequest(new UnifiedChatInput(List.of(input), "user", stream), chatCompletionModel);
    }

}
