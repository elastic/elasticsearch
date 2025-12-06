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
import org.elasticsearch.inference.InputType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.InputTypeTests;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.common.TruncatorTests;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioEndpointType;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioProvider;
import org.elasticsearch.xpack.inference.services.azureaistudio.embeddings.AzureAiStudioEmbeddingsModelTests;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.services.azureaistudio.request.AzureAiStudioEmbeddingsRequestEntity.convertToString;
import static org.elasticsearch.xpack.inference.services.azureopenai.request.AzureOpenAiUtils.API_KEY_HEADER;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class AzureAiStudioEmbeddingsRequestTests extends ESTestCase {

    public void testCreateRequest_WithOpenAiProvider_NoAdditionalParams() throws IOException {
        var inputType = InputTypeTests.randomSearchAndIngestWithNull();
        var request = createRequest(
            "http://openaitarget.local",
            AzureAiStudioProvider.OPENAI,
            AzureAiStudioEndpointType.TOKEN,
            "apikey",
            "abcd",
            null,
            inputType
        );
        var httpRequest = request.createHttpRequest();
        var httpPost = validateRequestUrlAndContentType(httpRequest, "http://openaitarget.local");
        validateRequestApiKey(httpPost, AzureAiStudioProvider.OPENAI, "apikey");

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(InputType.isSpecified(inputType) ? 2 : 1));
        assertThat(requestMap.get("input"), is(List.of("abcd")));
        AzureAiStudioEmbeddingsRequestTests.validateInputType(requestMap, inputType);

    }

    public void testCreateRequest_WithOpenAiProvider_WithUserParam() throws IOException {
        var inputType = InputTypeTests.randomSearchAndIngestWithNull();
        var request = createRequest(
            "http://openaitarget.local",
            AzureAiStudioProvider.OPENAI,
            AzureAiStudioEndpointType.TOKEN,
            "apikey",
            "abcd",
            "userid",
            inputType
        );
        var httpRequest = request.createHttpRequest();
        var httpPost = validateRequestUrlAndContentType(httpRequest, "http://openaitarget.local");
        validateRequestApiKey(httpPost, AzureAiStudioProvider.OPENAI, "apikey");

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(InputType.isSpecified(inputType) ? 3 : 2));
        assertThat(requestMap.get("input"), is(List.of("abcd")));
        assertThat(requestMap.get("user"), is("userid"));
        AzureAiStudioEmbeddingsRequestTests.validateInputType(requestMap, inputType);
    }

    public void testCreateRequest_WithCohereProvider_NoAdditionalParams() throws IOException {
        var inputType = InputTypeTests.randomSearchAndIngestWithNull();
        var request = createRequest(
            "http://coheretarget.local",
            AzureAiStudioProvider.COHERE,
            AzureAiStudioEndpointType.TOKEN,
            "apikey",
            "abcd",
            null,
            inputType
        );
        var httpRequest = request.createHttpRequest();
        var httpPost = validateRequestUrlAndContentType(httpRequest, "http://coheretarget.local/v1/embeddings");
        validateRequestApiKey(httpPost, AzureAiStudioProvider.COHERE, "apikey");

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(InputType.isSpecified(inputType) ? 2 : 1));
        assertThat(requestMap.get("input"), is(List.of("abcd")));
        AzureAiStudioEmbeddingsRequestTests.validateInputType(requestMap, inputType);
    }

    public void testCreateRequest_WithCohereProvider_WithUserParam() throws IOException {
        var inputType = InputTypeTests.randomSearchAndIngestWithNull();
        var request = createRequest(
            "http://coheretarget.local",
            AzureAiStudioProvider.COHERE,
            AzureAiStudioEndpointType.TOKEN,
            "apikey",
            "abcd",
            "userid",
            inputType
        );
        var httpRequest = request.createHttpRequest();
        var httpPost = validateRequestUrlAndContentType(httpRequest, "http://coheretarget.local/v1/embeddings");
        validateRequestApiKey(httpPost, AzureAiStudioProvider.COHERE, "apikey");

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(InputType.isSpecified(inputType) ? 3 : 2));
        assertThat(requestMap.get("input"), is(List.of("abcd")));
        assertThat(requestMap.get("user"), is("userid"));
        AzureAiStudioEmbeddingsRequestTests.validateInputType(requestMap, inputType);
    }

    public void testTruncate_ReducesInputTextSizeByHalf() throws IOException {
        var inputType = InputTypeTests.randomSearchAndIngestWithNull();
        var request = createRequest(
            "http://openaitarget.local",
            AzureAiStudioProvider.OPENAI,
            AzureAiStudioEndpointType.TOKEN,
            "apikey",
            "abcd",
            null,
            inputType
        );
        var truncatedRequest = request.truncate();

        var httpRequest = truncatedRequest.createHttpRequest();
        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();
        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(InputType.isSpecified(inputType) ? 2 : 1));
        assertThat(requestMap.get("input"), is(List.of("ab")));

    }

    public void testIsTruncated_ReturnsTrue() {
        var inputType = InputTypeTests.randomSearchAndIngestWithNull();
        var request = createRequest(
            "http://openaitarget.local",
            AzureAiStudioProvider.OPENAI,
            AzureAiStudioEndpointType.TOKEN,
            "apikey",
            "abcd",
            null,
            inputType
        );
        assertFalse(request.getTruncationInfo()[0]);

        var truncatedRequest = request.truncate();
        assertTrue(truncatedRequest.getTruncationInfo()[0]);
    }

    private HttpPost validateRequestUrlAndContentType(HttpRequest request, String expectedUrl) throws IOException {
        assertThat(request.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) request.httpRequestBase();
        assertThat(httpPost.getURI().toString(), is(expectedUrl));
        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        return httpPost;
    }

    private void validateRequestApiKey(HttpPost httpPost, AzureAiStudioProvider provider, String apiKey) {
        if (provider == AzureAiStudioProvider.OPENAI) {
            assertThat(httpPost.getLastHeader(API_KEY_HEADER).getValue(), is(apiKey));
        } else {
            assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is(apiKey));
        }
    }

    public static AzureAiStudioEmbeddingsRequest createRequest(
        String target,
        AzureAiStudioProvider provider,
        AzureAiStudioEndpointType endpointType,
        String apiKey,
        String input,
        @Nullable String user,
        InputType inputType
    ) {
        var model = AzureAiStudioEmbeddingsModelTests.createModel(
            "id",
            target,
            provider,
            endpointType,
            apiKey,
            null,
            false,
            null,
            null,
            user,
            null
        );
        return new AzureAiStudioEmbeddingsRequest(
            TruncatorTests.createTruncator(),
            new Truncator.TruncationResult(List.of(input), new boolean[] { false }),
            inputType,
            model
        );
    }

    public static void validateInputType(Map<String, Object> requestMap, InputType requestInputType) {
        if (InputType.isSpecified(requestInputType)) {
            var convertedInputType = convertToString(requestInputType);
            assertThat(requestMap.get("input_type"), is(convertedInputType));
        }
    }
}
