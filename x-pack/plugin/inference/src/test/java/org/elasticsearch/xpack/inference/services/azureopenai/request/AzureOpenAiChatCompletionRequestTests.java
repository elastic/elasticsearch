/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.request;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.azureopenai.completion.AzureOpenAiCompletionModelTests;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class AzureOpenAiChatCompletionRequestTests extends ESTestCase {
    // Completion field names
    private static final String N_FIELD_NAME = "n";
    private static final String STREAM_FIELD_NAME = "stream";
    private static final String MESSAGES_FIELD_NAME = "messages";
    private static final String ROLE_FIELD_NAME = "role";
    private static final String CONTENT_FIELD_NAME = "content";
    private static final String USER_FIELD_NAME = "user";
    private static final String STREAM_OPTIONS_FIELD_NAME = "stream_options";
    private static final String INCLUDE_USAGE_FIELD_NAME = "include_usage";
    private static final String API_KEY_HEADER_NAME = "api-key";
    // Test values
    private static final String ROLE_VALUE = "user";
    private static final String API_KEY_VALUE = "test_api_key";

    private static final String RESOURCE_NAME_VALUE = "some_resource_name";
    private static final String DEPLOYMENT_ID_VALUE = "some_deployment_id";
    private static final String SOME_API_VERSION = "some_api_version";
    private static final String URL_DEFAULT_VALUE = Strings.format(
        "https://%s.openai.azure.com/openai/deployments/%s/chat/completions?api-version=%s",
        RESOURCE_NAME_VALUE,
        DEPLOYMENT_ID_VALUE,
        SOME_API_VERSION
    );
    private static final String INFERENCE_ENTITY_ID_VALUE = "inferenceEntityId";
    private static final String USER_VALUE = "some_user";

    public void testCreateRequest_Streaming_ApiKey() throws IOException {
        String input = randomAlphaOfLength(15);
        var request = createRequest(input, true, API_KEY_VALUE, null);
        var httpPost = assertStreamingHttpPostCreated(request, input);
        assertThat(httpPost.getFirstHeader(API_KEY_HEADER_NAME).getValue(), is(API_KEY_VALUE));
    }

    public void testCreateRequest_Streaming_EntraId() throws IOException {
        String input = randomAlphaOfLength(15);
        var request = createRequest(input, true, null, API_KEY_VALUE);
        var httpPost = assertStreamingHttpPostCreated(request, input);
        assertThat(httpPost.getFirstHeader(HttpHeaders.AUTHORIZATION).getValue(), is(Strings.format("Bearer %s", API_KEY_VALUE)));
    }

    private static HttpPost assertStreamingHttpPostCreated(AzureOpenAiChatCompletionRequest request, String input) throws IOException {
        var httpRequest = request.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(request.getURI().toString(), is(URL_DEFAULT_VALUE));
        assertThat(requestMap.get(STREAM_FIELD_NAME), is(true));
        assertThat(requestMap.get(N_FIELD_NAME), is(1));
        assertThat(requestMap.get(MESSAGES_FIELD_NAME), is(List.of(Map.of(ROLE_FIELD_NAME, ROLE_VALUE, CONTENT_FIELD_NAME, input))));
        assertThat(requestMap.get(STREAM_OPTIONS_FIELD_NAME), is(Map.of(INCLUDE_USAGE_FIELD_NAME, true)));
        assertThat(requestMap.get(USER_FIELD_NAME), is(USER_VALUE));
        assertThat(requestMap, aMapWithSize(5));
        return httpPost;
    }

    public void testCreateRequest_NonStreaming_ApiKey() throws IOException {
        String input = randomAlphaOfLength(15);
        var request = createRequest(input, false, API_KEY_VALUE, null);
        var httpPost = assertNonStreamingHttpPostCreated(request, input);
        assertThat(httpPost.getFirstHeader(API_KEY_HEADER_NAME).getValue(), is(API_KEY_VALUE));
    }

    public void testCreateRequest_NonStreaming_EntraId() throws IOException {
        String input = randomAlphaOfLength(15);
        var request = createRequest(input, false, null, API_KEY_VALUE);
        var httpPost = assertNonStreamingHttpPostCreated(request, input);
        assertThat(httpPost.getFirstHeader(HttpHeaders.AUTHORIZATION).getValue(), is(Strings.format("Bearer %s", API_KEY_VALUE)));
    }

    private static HttpPost assertNonStreamingHttpPostCreated(AzureOpenAiChatCompletionRequest request, String input) throws IOException {
        var httpRequest = request.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(request.getURI().toString(), is(URL_DEFAULT_VALUE));
        assertThat(requestMap.get(STREAM_FIELD_NAME), is(false));
        assertThat(requestMap.get(N_FIELD_NAME), is(1));
        assertThat(requestMap.get(USER_FIELD_NAME), is(USER_VALUE));
        assertThat(requestMap.get(MESSAGES_FIELD_NAME), is(List.of(Map.of(ROLE_FIELD_NAME, ROLE_VALUE, CONTENT_FIELD_NAME, input))));
        assertThat(requestMap, aMapWithSize(4));
        return httpPost;
    }

    public void testTruncate_DoesNotReduceInputTextSize() {
        String input = randomAlphaOfLength(5);
        var request = createRequest(input, true, API_KEY_VALUE, null);
        assertThat(request.truncate(), is(sameInstance(request)));
    }

    public void testGetUrl_ReturnsDefault() {
        var request = createRequest(randomAlphaOfLength(5), true, API_KEY_VALUE, null);
        assertThat(request.getURI().toString(), is(URL_DEFAULT_VALUE));
    }

    private static AzureOpenAiChatCompletionRequest createRequest(String input, boolean stream, String apiKey, String entraId) {
        var chatCompletionModel = AzureOpenAiCompletionModelTests.createChatCompletionModel(
            RESOURCE_NAME_VALUE,
            DEPLOYMENT_ID_VALUE,
            SOME_API_VERSION,
            USER_VALUE,
            apiKey,
            entraId,
            INFERENCE_ENTITY_ID_VALUE
        );
        return new AzureOpenAiChatCompletionRequest(new UnifiedChatInput(List.of(input), ROLE_VALUE, stream), chatCompletionModel);
    }

}
