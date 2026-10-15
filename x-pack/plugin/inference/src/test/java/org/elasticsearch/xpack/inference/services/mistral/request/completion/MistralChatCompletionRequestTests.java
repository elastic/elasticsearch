/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mistral.request.completion;

import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnifiedCompletionRequestBody;
import org.elasticsearch.inference.completion.ContentString;
import org.elasticsearch.inference.completion.Message;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.external.request.RequestTests;
import org.elasticsearch.xpack.inference.services.mistral.MistralConstants;
import org.elasticsearch.xpack.inference.services.mistral.completion.MistralChatCompletionModel;
import org.elasticsearch.xpack.inference.services.mistral.completion.MistralChatCompletionModelTests;
import org.elasticsearch.xpack.inference.services.mistral.completion.MistralChatCompletionServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

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
        var httpRequest = RequestTests.getHttpRequestSync(request);

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap.get("stream"), is(true));
    }

    public void testCreateRequest_SendsServiceSettingReasoningEffortAndAdaptedBody() throws IOException {
        var model = new MistralChatCompletionModel(
            "id",
            TaskType.CHAT_COMPLETION,
            "service",
            new MistralChatCompletionServiceSettings("mistral-reasoning-test", null, "high"),
            new DefaultSecretSettings(new SecureString("secret".toCharArray()))
        );
        var body = new UnifiedCompletionRequestBody(
            List.of(new Message(new ContentString("hi"), "developer", null, null)),
            null,
            null,
            null,
            0f,
            null,
            null,
            null
        );
        var request = new MistralChatCompletionRequest(new UnifiedChatInput(body, true), model);

        var httpPost = (HttpPost) RequestTests.getHttpRequestSync(request).httpRequestBase();
        var requestMap = entityAsMap(httpPost.getEntity().getContent());

        assertThat(requestMap.get("reasoning_effort"), is("high"));
        assertThat(requestMap.get("top_p"), is(1.0));
        assertThat(requestMap.get("messages"), is(List.of(Map.of("role", "system", "content", "hi"))));
    }

    public void testTruncate_DoesNotReduceInputTextSize() throws IOException {
        String input = randomAlphaOfLength(5);
        var request = createRequest("secret", input, "model", true);
        var truncatedRequest = request.truncate();
        assertThat(request.getURI().toString(), is(MistralConstants.API_COMPLETIONS_PATH));

        var httpRequest = RequestTests.getHttpRequestSync(truncatedRequest);
        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();
        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(4));

        // We do not truncate for Mistral chat completions
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
