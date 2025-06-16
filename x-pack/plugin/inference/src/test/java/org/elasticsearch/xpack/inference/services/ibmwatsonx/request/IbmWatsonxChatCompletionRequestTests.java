/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx.request;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.completion.IbmWatsonxChatCompletionModel;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.completion.IbmWatsonxChatCompletionModelTests;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class IbmWatsonxChatCompletionRequestTests extends ESTestCase {
    private static final String AUTH_HEADER_VALUE = "foo";
    private static final String API_COMPLETIONS_PATH = "https://abc.com/ml/v1/text/chat?version=apiVersion";

    public void testCreateRequest_WithStreaming() throws IOException, URISyntaxException {
        var request = createRequest("secret", randomAlphaOfLength(15), "model", true);
        var httpRequest = request.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap.get("stream"), is(true));
    }

    public void testTruncate_DoesNotReduceInputTextSize() throws IOException, URISyntaxException {
        String input = randomAlphaOfLength(5);
        var request = createRequest("secret", input, "model", true);
        var truncatedRequest = request.truncate();
        assertThat(request.getURI().toString(), is(API_COMPLETIONS_PATH));

        var httpRequest = truncatedRequest.createHttpRequest();
        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();
        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(5));

        assertThat(requestMap.get("messages"), is(List.of(Map.of("role", "user", "content", input))));
        assertThat(requestMap.get("model"), is("model"));
        assertThat(requestMap.get("n"), is(1));
        assertTrue((Boolean) requestMap.get("stream"));
        assertNull(requestMap.get("stream_options"));
    }

    public void testTruncationInfo_ReturnsNull() throws URISyntaxException {
        var request = createRequest("secret", randomAlphaOfLength(5), "model", true);
        assertNull(request.getTruncationInfo());
    }

    public static IbmWatsonxChatCompletionRequest createRequest(String apiKey, String input, @Nullable String model)
        throws URISyntaxException {
        return createRequest(apiKey, input, model, false);
    }

    public static IbmWatsonxChatCompletionRequest createRequest(String apiKey, String input, @Nullable String model, boolean stream)
        throws URISyntaxException {
        var chatCompletionModel = IbmWatsonxChatCompletionModelTests.createModel(
            new URI("abc.com"),
            "apiVersion",
            model,
            "projectId",
            apiKey
        );
        return new IbmWatsonxChatCompletionWithoutAuthRequest(new UnifiedChatInput(List.of(input), "user", stream), chatCompletionModel);
    }

    private static class IbmWatsonxChatCompletionWithoutAuthRequest extends IbmWatsonxChatCompletionRequest {
        IbmWatsonxChatCompletionWithoutAuthRequest(UnifiedChatInput input, IbmWatsonxChatCompletionModel model) {
            super(input, model);
        }

        @Override
        public void decorateWithAuth(HttpPost httpPost) {
            httpPost.setHeader(HttpHeaders.AUTHORIZATION, AUTH_HEADER_VALUE);
        }
    }
}
