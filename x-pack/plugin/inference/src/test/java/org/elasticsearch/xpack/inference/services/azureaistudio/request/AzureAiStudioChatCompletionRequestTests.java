/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaistudio.request;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioEndpointType;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioProvider;
import org.elasticsearch.xpack.inference.services.azureaistudio.completion.AzureAiStudioChatCompletionModelTests;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.services.azureopenai.request.AzureOpenAiUtils.API_KEY_HEADER;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class AzureAiStudioChatCompletionRequestTests extends ESTestCase {

    public void testCreateRequest_WithOpenAiProviderTokenEndpoint_NoParams() throws IOException {
        var request = createRequest(
            "http://openaitarget.local",
            AzureAiStudioProvider.OPENAI,
            AzureAiStudioEndpointType.TOKEN,
            "apikey",
            "abcd"
        );
        var httpRequest = request.createHttpRequest();

        var httpPost = validateRequestUrlAndContentType(httpRequest, "http://openaitarget.local");
        validateRequestApiKey(httpPost, AzureAiStudioProvider.OPENAI, AzureAiStudioEndpointType.TOKEN, "apikey");

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(1));
        assertThat(requestMap.get("messages"), is(List.of(Map.of("role", "user", "content", "abcd"))));
    }

    public void testCreateRequest_WithOpenAiProviderTokenEndpoint_WithTemperatureParam() throws IOException {
        var request = createRequest(
            "http://openaitarget.local",
            AzureAiStudioProvider.OPENAI,
            AzureAiStudioEndpointType.TOKEN,
            "apikey",
            1.0,
            null,
            null,
            null,
            "abcd"
        );
        var httpRequest = request.createHttpRequest();

        var httpPost = validateRequestUrlAndContentType(httpRequest, "http://openaitarget.local");
        validateRequestApiKey(httpPost, AzureAiStudioProvider.OPENAI, AzureAiStudioEndpointType.TOKEN, "apikey");

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(2));
        assertThat(requestMap.get("messages"), is(List.of(Map.of("role", "user", "content", "abcd"))));
        assertThat(requestMap.get("parameters"), is(getParameterMap(1.0, null, null, null)));
    }

    public void testCreateRequest_WithOpenAiProviderTokenEndpoint_WithTopPParam() throws IOException {
        var request = createRequest(
            "http://openaitarget.local",
            AzureAiStudioProvider.OPENAI,
            AzureAiStudioEndpointType.TOKEN,
            "apikey",
            null,
            2.0,
            null,
            null,
            "abcd"
        );
        var httpRequest = request.createHttpRequest();

        var httpPost = validateRequestUrlAndContentType(httpRequest, "http://openaitarget.local");
        validateRequestApiKey(httpPost, AzureAiStudioProvider.OPENAI, AzureAiStudioEndpointType.TOKEN, "apikey");

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(2));
        assertThat(requestMap.get("messages"), is(List.of(Map.of("role", "user", "content", "abcd"))));
        assertThat(requestMap.get("parameters"), is(getParameterMap(null, 2.0, null, null)));
    }

    public void testCreateRequest_WithOpenAiProviderTokenEndpoint_WithDoSampleParam() throws IOException {
        var request = createRequest(
            "http://openaitarget.local",
            AzureAiStudioProvider.OPENAI,
            AzureAiStudioEndpointType.TOKEN,
            "apikey",
            null,
            null,
            true,
            null,
            "abcd"
        );
        var httpRequest = request.createHttpRequest();

        var httpPost = validateRequestUrlAndContentType(httpRequest, "http://openaitarget.local");
        validateRequestApiKey(httpPost, AzureAiStudioProvider.OPENAI, AzureAiStudioEndpointType.TOKEN, "apikey");

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(2));
        assertThat(requestMap.get("messages"), is(List.of(Map.of("role", "user", "content", "abcd"))));
        assertThat(requestMap.get("parameters"), is(getParameterMap(null, null, true, null)));
    }

    public void testCreateRequest_WithOpenAiProviderTokenEndpoint_WithMaxNewTokensParam() throws IOException {
        var request = createRequest(
            "http://openaitarget.local",
            AzureAiStudioProvider.OPENAI,
            AzureAiStudioEndpointType.TOKEN,
            "apikey",
            null,
            null,
            null,
            512,
            "abcd"
        );
        var httpRequest = request.createHttpRequest();

        var httpPost = validateRequestUrlAndContentType(httpRequest, "http://openaitarget.local");
        validateRequestApiKey(httpPost, AzureAiStudioProvider.OPENAI, AzureAiStudioEndpointType.TOKEN, "apikey");

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(2));
        assertThat(requestMap.get("messages"), is(List.of(Map.of("role", "user", "content", "abcd"))));
        assertThat(requestMap.get("parameters"), is(getParameterMap(null, null, null, 512)));
    }

    public void testCreateRequest_WithCohereProviderTokenEndpoint_NoParams() throws IOException {
        var request = createRequest(
            "http://coheretarget.local",
            AzureAiStudioProvider.COHERE,
            AzureAiStudioEndpointType.TOKEN,
            "apikey",
            "abcd"
        );
        var httpRequest = request.createHttpRequest();

        var httpPost = validateRequestUrlAndContentType(httpRequest, "http://coheretarget.local/v1/chat/completions");
        validateRequestApiKey(httpPost, AzureAiStudioProvider.COHERE, AzureAiStudioEndpointType.TOKEN, "apikey");

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(1));
        assertThat(requestMap.get("messages"), is(List.of(Map.of("role", "user", "content", "abcd"))));
    }

    public void testCreateRequest_WithCohereProviderTokenEndpoint_WithTemperatureParam() throws IOException {
        var request = createRequest(
            "http://coheretarget.local",
            AzureAiStudioProvider.COHERE,
            AzureAiStudioEndpointType.TOKEN,
            "apikey",
            1.0,
            null,
            null,
            null,
            "abcd"
        );
        var httpRequest = request.createHttpRequest();

        var httpPost = validateRequestUrlAndContentType(httpRequest, "http://coheretarget.local/v1/chat/completions");
        validateRequestApiKey(httpPost, AzureAiStudioProvider.COHERE, AzureAiStudioEndpointType.TOKEN, "apikey");

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(2));
        assertThat(requestMap.get("messages"), is(List.of(Map.of("role", "user", "content", "abcd"))));
        assertThat(requestMap.get("parameters"), is(getParameterMap(1.0, null, null, null)));
    }

    public void testCreateRequest_WithCohereProviderTokenEndpoint_WithTopPParam() throws IOException {
        var request = createRequest(
            "http://coheretarget.local",
            AzureAiStudioProvider.COHERE,
            AzureAiStudioEndpointType.TOKEN,
            "apikey",
            null,
            2.0,
            null,
            null,
            "abcd"
        );
        var httpRequest = request.createHttpRequest();

        var httpPost = validateRequestUrlAndContentType(httpRequest, "http://coheretarget.local/v1/chat/completions");
        validateRequestApiKey(httpPost, AzureAiStudioProvider.COHERE, AzureAiStudioEndpointType.TOKEN, "apikey");

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(2));
        assertThat(requestMap.get("messages"), is(List.of(Map.of("role", "user", "content", "abcd"))));
        assertThat(requestMap.get("parameters"), is(getParameterMap(null, 2.0, null, null)));
    }

    public void testCreateRequest_WithCohereProviderTokenEndpoint_WithDoSampleParam() throws IOException {
        var request = createRequest(
            "http://coheretarget.local",
            AzureAiStudioProvider.COHERE,
            AzureAiStudioEndpointType.TOKEN,
            "apikey",
            null,
            null,
            true,
            null,
            "abcd"
        );
        var httpRequest = request.createHttpRequest();

        var httpPost = validateRequestUrlAndContentType(httpRequest, "http://coheretarget.local/v1/chat/completions");
        validateRequestApiKey(httpPost, AzureAiStudioProvider.COHERE, AzureAiStudioEndpointType.TOKEN, "apikey");

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(2));
        assertThat(requestMap.get("messages"), is(List.of(Map.of("role", "user", "content", "abcd"))));
        assertThat(requestMap.get("parameters"), is(getParameterMap(null, null, true, null)));
    }

    public void testCreateRequest_WithCohereProviderTokenEndpoint_WithMaxNewTokensParam() throws IOException {
        var request = createRequest(
            "http://coheretarget.local",
            AzureAiStudioProvider.COHERE,
            AzureAiStudioEndpointType.TOKEN,
            "apikey",
            null,
            null,
            null,
            512,
            "abcd"
        );
        var httpRequest = request.createHttpRequest();

        var httpPost = validateRequestUrlAndContentType(httpRequest, "http://coheretarget.local/v1/chat/completions");
        validateRequestApiKey(httpPost, AzureAiStudioProvider.COHERE, AzureAiStudioEndpointType.TOKEN, "apikey");

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(2));
        assertThat(requestMap.get("messages"), is(List.of(Map.of("role", "user", "content", "abcd"))));
        assertThat(requestMap.get("parameters"), is(getParameterMap(null, null, null, 512)));
    }

    public void testCreateRequest_WithMistralProviderRealtimeEndpoint_NoParams() throws IOException {
        var request = createRequest(
            "http://mistral.local/score",
            AzureAiStudioProvider.MISTRAL,
            AzureAiStudioEndpointType.REALTIME,
            "apikey",
            "abcd"
        );
        var httpRequest = request.createHttpRequest();

        var httpPost = validateRequestUrlAndContentType(httpRequest, "http://mistral.local/score");
        validateRequestApiKey(httpPost, AzureAiStudioProvider.MISTRAL, AzureAiStudioEndpointType.REALTIME, "apikey");

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(1));

        @SuppressWarnings("unchecked")
        var input_data = (Map<String, Object>) requestMap.get("input_data");
        assertThat(input_data, aMapWithSize(1));
        assertThat(input_data.get("input_string"), is(List.of(Map.of("role", "user", "content", "abcd"))));
    }

    public void testCreateRequest_WithMistralProviderRealtimeEndpoint_WithTemperatureParam() throws IOException {
        var request = createRequest(
            "http://mistral.local/score",
            AzureAiStudioProvider.MISTRAL,
            AzureAiStudioEndpointType.REALTIME,
            "apikey",
            1.0,
            null,
            null,
            null,
            "abcd"
        );
        var httpRequest = request.createHttpRequest();

        var httpPost = validateRequestUrlAndContentType(httpRequest, "http://mistral.local/score");
        validateRequestApiKey(httpPost, AzureAiStudioProvider.MISTRAL, AzureAiStudioEndpointType.REALTIME, "apikey");

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(1));

        @SuppressWarnings("unchecked")
        var input_data = (Map<String, Object>) requestMap.get("input_data");
        assertThat(input_data, aMapWithSize(2));
        assertThat(input_data.get("input_string"), is(List.of(Map.of("role", "user", "content", "abcd"))));
        assertThat(input_data.get("parameters"), is(getParameterMap(1.0, null, null, null)));
    }

    public void testCreateRequest_WithMistralProviderRealtimeEndpoint_WithTopPParam() throws IOException {
        var request = createRequest(
            "http://mistral.local/score",
            AzureAiStudioProvider.MISTRAL,
            AzureAiStudioEndpointType.REALTIME,
            "apikey",
            null,
            2.0,
            null,
            null,
            "abcd"
        );
        var httpRequest = request.createHttpRequest();

        var httpPost = validateRequestUrlAndContentType(httpRequest, "http://mistral.local/score");
        validateRequestApiKey(httpPost, AzureAiStudioProvider.MISTRAL, AzureAiStudioEndpointType.REALTIME, "apikey");

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(1));

        @SuppressWarnings("unchecked")
        var input_data = (Map<String, Object>) requestMap.get("input_data");
        assertThat(input_data, aMapWithSize(2));
        assertThat(input_data.get("input_string"), is(List.of(Map.of("role", "user", "content", "abcd"))));
        assertThat(input_data.get("parameters"), is(getParameterMap(null, 2.0, null, null)));
    }

    public void testCreateRequest_WithMistralProviderRealtimeEndpoint_WithDoSampleParam() throws IOException {
        var request = createRequest(
            "http://mistral.local/score",
            AzureAiStudioProvider.MISTRAL,
            AzureAiStudioEndpointType.REALTIME,
            "apikey",
            null,
            null,
            true,
            null,
            "abcd"
        );
        var httpRequest = request.createHttpRequest();

        var httpPost = validateRequestUrlAndContentType(httpRequest, "http://mistral.local/score");
        validateRequestApiKey(httpPost, AzureAiStudioProvider.MISTRAL, AzureAiStudioEndpointType.REALTIME, "apikey");

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(1));

        @SuppressWarnings("unchecked")
        var input_data = (Map<String, Object>) requestMap.get("input_data");
        assertThat(input_data, aMapWithSize(2));
        assertThat(input_data.get("input_string"), is(List.of(Map.of("role", "user", "content", "abcd"))));
        assertThat(input_data.get("parameters"), is(getParameterMap(null, null, true, null)));
    }

    public void testCreateRequest_WithMistralProviderRealtimeEndpoint_WithMaxNewTokensParam() throws IOException {
        var request = createRequest(
            "http://mistral.local/score",
            AzureAiStudioProvider.MISTRAL,
            AzureAiStudioEndpointType.REALTIME,
            "apikey",
            null,
            null,
            null,
            512,
            "abcd"
        );
        var httpRequest = request.createHttpRequest();

        var httpPost = validateRequestUrlAndContentType(httpRequest, "http://mistral.local/score");
        validateRequestApiKey(httpPost, AzureAiStudioProvider.MISTRAL, AzureAiStudioEndpointType.REALTIME, "apikey");

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(1));

        @SuppressWarnings("unchecked")
        var input_data = (Map<String, Object>) requestMap.get("input_data");
        assertThat(input_data, aMapWithSize(2));
        assertThat(input_data.get("input_string"), is(List.of(Map.of("role", "user", "content", "abcd"))));
        assertThat(input_data.get("parameters"), is(getParameterMap(null, null, null, 512)));
    }

    private HttpPost validateRequestUrlAndContentType(HttpRequest request, String expectedUrl) throws IOException {
        assertThat(request.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) request.httpRequestBase();
        assertThat(httpPost.getURI().toString(), is(expectedUrl));
        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        return httpPost;
    }

    private void validateRequestApiKey(
        HttpPost httpPost,
        AzureAiStudioProvider provider,
        AzureAiStudioEndpointType endpointType,
        String apiKey
    ) {
        if (endpointType == AzureAiStudioEndpointType.TOKEN) {
            if (provider == AzureAiStudioProvider.OPENAI) {
                assertThat(httpPost.getLastHeader(API_KEY_HEADER).getValue(), is(apiKey));
            } else {
                assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is(apiKey));
            }
        } else {
            assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer " + apiKey));
        }
    }

    private Map<String, Object> getParameterMap(
        @Nullable Double temperature,
        @Nullable Double topP,
        @Nullable Boolean doSample,
        @Nullable Integer maxNewTokens
    ) {
        var map = new HashMap<String, Object>();
        if (temperature != null) {
            map.put("temperature", temperature);
        }
        if (topP != null) {
            map.put("top_p", topP);
        }
        if (doSample != null) {
            map.put("do_sample", doSample);
        }
        if (maxNewTokens != null) {
            map.put("max_new_tokens", maxNewTokens);
        }
        return map;
    }

    public static AzureAiStudioChatCompletionRequest createRequest(
        String target,
        AzureAiStudioProvider provider,
        AzureAiStudioEndpointType endpointType,
        String apiKey,
        String input
    ) {
        return createRequest(target, provider, endpointType, apiKey, null, null, null, null, input);
    }

    public static AzureAiStudioChatCompletionRequest createRequest(
        String target,
        AzureAiStudioProvider provider,
        AzureAiStudioEndpointType endpointType,
        String apiKey,
        @Nullable Double temperature,
        @Nullable Double topP,
        @Nullable Boolean doSample,
        @Nullable Integer maxNewTokens,
        String input
    ) {
        var model = AzureAiStudioChatCompletionModelTests.createModel(
            "id",
            target,
            provider,
            endpointType,
            apiKey,
            temperature,
            topP,
            doSample,
            maxNewTokens,
            null
        );
        return new AzureAiStudioChatCompletionRequest(model, List.of(input), false);
    }
}
