/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.azureaistudio;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.common.TruncatorTests;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioEndpointType;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioProvider;
import org.elasticsearch.xpack.inference.services.azureaistudio.embeddings.AzureAiStudioEmbeddingsModelTests;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.request.azureopenai.AzureOpenAiUtils.API_KEY_HEADER;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class AzureAiStudioEmbeddingsRequestTests extends ESTestCase {

    public void testCreateRequest_WithOpenAiProvider_NoAdditionalParams() throws IOException {
        var request = createRequest(
            "http://openaitarget.local",
            AzureAiStudioProvider.OPENAI,
            AzureAiStudioEndpointType.TOKEN,
            "apikey",
            "abcd",
            null
        );
        var httpRequest = request.createHttpRequest();
        var httpPost = validateRequestUrlAndContentType(httpRequest, "http://openaitarget.local");
        validateRequestApiKey(httpPost, AzureAiStudioProvider.OPENAI, "apikey");

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(1));
        assertThat(requestMap.get("input"), is(List.of("abcd")));
    }

    public void testCreateRequest_WithOpenAiProvider_WithUserParam() throws IOException {
        var request = createRequest(
            "http://openaitarget.local",
            AzureAiStudioProvider.OPENAI,
            AzureAiStudioEndpointType.TOKEN,
            "apikey",
            "abcd",
            "userid"
        );
        var httpRequest = request.createHttpRequest();
        var httpPost = validateRequestUrlAndContentType(httpRequest, "http://openaitarget.local");
        validateRequestApiKey(httpPost, AzureAiStudioProvider.OPENAI, "apikey");

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(2));
        assertThat(requestMap.get("input"), is(List.of("abcd")));
        assertThat(requestMap.get("user"), is("userid"));
    }

    public void testCreateRequest_WithCohereProvider_NoAdditionalParams() throws IOException {
        var request = createRequest(
            "http://coheretarget.local",
            AzureAiStudioProvider.COHERE,
            AzureAiStudioEndpointType.TOKEN,
            "apikey",
            "abcd",
            null
        );
        var httpRequest = request.createHttpRequest();
        var httpPost = validateRequestUrlAndContentType(httpRequest, "http://coheretarget.local/v1/embeddings");
        validateRequestApiKey(httpPost, AzureAiStudioProvider.COHERE, "apikey");

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(1));
        assertThat(requestMap.get("input"), is(List.of("abcd")));
    }

    public void testCreateRequest_WithCohereProvider_WithUserParam() throws IOException {
        var request = createRequest(
            "http://coheretarget.local",
            AzureAiStudioProvider.COHERE,
            AzureAiStudioEndpointType.TOKEN,
            "apikey",
            "abcd",
            "userid"
        );
        var httpRequest = request.createHttpRequest();
        var httpPost = validateRequestUrlAndContentType(httpRequest, "http://coheretarget.local/v1/embeddings");
        validateRequestApiKey(httpPost, AzureAiStudioProvider.COHERE, "apikey");

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(2));
        assertThat(requestMap.get("input"), is(List.of("abcd")));
        assertThat(requestMap.get("user"), is("userid"));
    }

    public void testTruncate_ReducesInputTextSizeByHalf() throws IOException {
        var request = createRequest(
            "http://openaitarget.local",
            AzureAiStudioProvider.OPENAI,
            AzureAiStudioEndpointType.TOKEN,
            "apikey",
            "abcd",
            null
        );
        var truncatedRequest = request.truncate();

        var httpRequest = truncatedRequest.createHttpRequest();
        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();
        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(1));
        assertThat(requestMap.get("input"), is(List.of("ab")));
    }

    public void testIsTruncated_ReturnsTrue() {
        var request = createRequest(
            "http://openaitarget.local",
            AzureAiStudioProvider.OPENAI,
            AzureAiStudioEndpointType.TOKEN,
            "apikey",
            "abcd",
            null
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
        @Nullable String user
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
            model
        );
    }
}
