/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mistral.request.completion;

import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.mistral.MistralConstants;
import org.elasticsearch.xpack.inference.services.mistral.completion.MistralChatCompletionModelTests;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class MistralChatCompletionRequestTests extends ESTestCase {

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
        assertThat(request.getURI().toString(), is(MistralConstants.API_COMPLETIONS_PATH));

        var httpRequest = truncatedRequest.createHttpRequest();
        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();
        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(4));

        // We do not truncate for Hugging Face chat completions
        assertThat(requestMap.get("messages"), is(List.of(Map.of("role", "user", "content", input))));
        assertThat(requestMap.get("model"), is("model"));
        assertThat(requestMap.get("n"), is(1));
        assertTrue((Boolean) requestMap.get("stream"));
        assertNull(requestMap.get("stream_options")); // Mistral does not use stream options
    }

    public void testTruncationInfo_ReturnsNull() {
        var request = createRequest("secret", randomAlphaOfLength(5), "model", true);
        assertNull(request.getTruncationInfo());
    }

    public static MistralChatCompletionRequest createRequest(String apiKey, String input, @Nullable String model) {
        return createRequest(apiKey, input, model, false);
    }

    public static MistralChatCompletionRequest createRequest(String apiKey, String input, @Nullable String model, boolean stream) {
        var chatCompletionModel = MistralChatCompletionModelTests.createCompletionModel(apiKey, model);
        return new MistralChatCompletionRequest(new UnifiedChatInput(List.of(input), "user", stream), chatCompletionModel);
    }

}
