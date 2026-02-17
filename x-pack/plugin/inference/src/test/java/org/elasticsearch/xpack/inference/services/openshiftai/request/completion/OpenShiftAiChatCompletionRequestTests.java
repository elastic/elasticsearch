/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openshiftai.request.completion;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.openshiftai.completion.OpenShiftAiChatCompletionModelTests;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class OpenShiftAiChatCompletionRequestTests extends ESTestCase {

    // Completion field names
    private static final String N_FIELD_NAME = "n";
    private static final String STREAM_FIELD_NAME = "stream";
    private static final String MESSAGES_FIELD_NAME = "messages";
    private static final String ROLE_FIELD_NAME = "role";
    private static final String CONTENT_FIELD_NAME = "content";
    private static final String MODEL_FIELD_NAME = "model";

    // Test values
    private static final String URL_VALUE = "http://www.abc.com";
    private static final String MODEL_VALUE = "some_model";
    private static final String ROLE_VALUE = "user";
    private static final String API_KEY_VALUE = "test_api_key";

    public void testCreateRequest_WithStreaming() throws IOException {
        String input = randomAlphaOfLength(15);
        var request = createRequest(MODEL_VALUE, URL_VALUE, API_KEY_VALUE, input, true);
        var httpRequest = request.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(request.getURI().toString(), is(URL_VALUE));
        assertThat(requestMap.get(STREAM_FIELD_NAME), is(true));
        assertThat(requestMap.get(MODEL_FIELD_NAME), is(MODEL_VALUE));
        assertThat(requestMap.get(N_FIELD_NAME), is(1));
        assertThat(requestMap.get(MESSAGES_FIELD_NAME), is(List.of(Map.of(ROLE_FIELD_NAME, ROLE_VALUE, CONTENT_FIELD_NAME, input))));
        assertThat(requestMap, aMapWithSize(4));
        assertThat(httpPost.getFirstHeader(HttpHeaders.AUTHORIZATION).getValue(), is(Strings.format("Bearer %s", API_KEY_VALUE)));
    }

    public void testTruncate_DoesNotReduceInputTextSize() {
        String input = randomAlphaOfLength(5);
        var request = createRequest(MODEL_VALUE, URL_VALUE, API_KEY_VALUE, input, true);
        assertThat(request.truncate(), is(sameInstance(request)));
    }

    public void testTruncationInfo_ReturnsNull() {
        var request = createRequest(MODEL_VALUE, URL_VALUE, API_KEY_VALUE, randomAlphaOfLength(5), true);
        assertThat(request.getTruncationInfo(), is(nullValue()));
    }

    public static OpenShiftAiChatCompletionRequest createRequest(String modelId, String url, String apiKey, String input, boolean stream) {
        var chatCompletionModel = OpenShiftAiChatCompletionModelTests.createChatCompletionModel(url, apiKey, modelId);
        return new OpenShiftAiChatCompletionRequest(new UnifiedChatInput(List.of(input), ROLE_VALUE, stream), chatCompletionModel);
    }

}
