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
import org.elasticsearch.xpack.inference.services.azureaistudio.rerank.AzureAiStudioRerankModelTests;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.TOP_N_FIELD;
import static org.elasticsearch.xpack.inference.services.azureopenai.request.AzureOpenAiUtils.API_KEY_HEADER;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class AzureAiStudioRerankRequestTests extends ESTestCase {
    private static final String TARGET_URI = "http://testtarget.local";
    private static final String INPUT = "documents";
    private static final String QUERY = "query";
    private static final Integer TOP_N = 2;

    public void testCreateRequest_WithCohereProviderTokenEndpoint_NoParams() throws IOException {
        final var input = randomAlphaOfLength(3);
        final var query = randomAlphaOfLength(3);
        final var apikey = randomAlphaOfLength(3);
        final var request = createRequest(TARGET_URI, AzureAiStudioProvider.COHERE, AzureAiStudioEndpointType.TOKEN, apikey, query, input);
        final var httpRequest = request.createHttpRequest();

        final var httpPost = validateRequestUrlAndContentType(httpRequest, TARGET_URI + "/v1/rerank");
        validateRequestApiKey(httpPost, AzureAiStudioProvider.COHERE, AzureAiStudioEndpointType.TOKEN, apikey);

        final var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(3));
        assertThat(requestMap.get(QUERY), is(query));
        assertThat(requestMap.get(INPUT), is(List.of(input)));
    }

    public void testCreateRequest_WithCohereProviderTokenEndpoint_WithTopNParam() throws IOException {
        final var input = randomAlphaOfLength(3);
        final var query = randomAlphaOfLength(3);
        final var apikey = randomAlphaOfLength(3);
        final var request = createRequest(
            TARGET_URI,
            AzureAiStudioProvider.COHERE,
            AzureAiStudioEndpointType.TOKEN,
            apikey,
            null,
            TOP_N,
            query,
            input
        );
        final var httpRequest = request.createHttpRequest();

        final var httpPost = validateRequestUrlAndContentType(httpRequest, TARGET_URI + "/v1/rerank");
        validateRequestApiKey(httpPost, AzureAiStudioProvider.COHERE, AzureAiStudioEndpointType.TOKEN, apikey);

        final var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(4));
        assertThat(requestMap.get(QUERY), is(query));
        assertThat(requestMap.get(INPUT), is(List.of(input)));
        assertThat(requestMap.get(TOP_N_FIELD), is(TOP_N));
    }

    private HttpPost validateRequestUrlAndContentType(HttpRequest request, String expectedUrl) {
        assertThat(request.httpRequestBase(), instanceOf(HttpPost.class));
        final var httpPost = (HttpPost) request.httpRequestBase();
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

    public static AzureAiStudioRerankRequest createRequest(
        String target,
        AzureAiStudioProvider provider,
        AzureAiStudioEndpointType endpointType,
        String apiKey,
        String query,
        String input
    ) {
        return createRequest(target, provider, endpointType, apiKey, null, null, query, input);
    }

    public static AzureAiStudioRerankRequest createRequest(
        String target,
        AzureAiStudioProvider provider,
        AzureAiStudioEndpointType endpointType,
        String apiKey,
        @Nullable Boolean returnDocuments,
        @Nullable Integer topN,
        String query,
        String input
    ) {
        final var model = AzureAiStudioRerankModelTests.createModel(
            "id",
            target,
            provider,
            endpointType,
            apiKey,
            returnDocuments,
            topN,
            null
        );
        return new AzureAiStudioRerankRequest(model, query, List.of(input), false, topN);
    }
}
