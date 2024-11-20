/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.azureopenai.embeddings;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.common.TruncatorTests;
import org.elasticsearch.xpack.inference.external.request.azureopenai.AzureOpenAiEmbeddingsRequest;
import org.elasticsearch.xpack.inference.services.azureopenai.embeddings.AzureOpenAiEmbeddingsModelTests;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.request.azureopenai.AzureOpenAiUtils.API_KEY_HEADER;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class AzureOpenAiEmbeddingsRequestTests extends ESTestCase {

    public void testCreateRequest_WithApiKeyDefined() throws IOException {
        var input = "input";
        var user = "user";
        var apiKey = randomAlphaOfLength(10);

        var request = createRequest("resource", "deployment", "2024", apiKey, null, input, user);
        var httpRequest = request.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(
            httpPost.getURI().toString(),
            is("https://resource.openai.azure.com/openai/deployments/deployment/embeddings?api-version=2024")
        );

        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        assertThat(httpPost.getLastHeader(API_KEY_HEADER).getValue(), is(apiKey));

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap.size(), equalTo(2));
        assertThat(requestMap.get("input"), is(List.of(input)));
        assertThat(requestMap.get("user"), is(user));
    }

    public void testCreateRequest_WithEntraIdDefined() throws IOException {
        var input = "input";
        var user = "user";
        var entraId = randomAlphaOfLength(10);

        var request = createRequest("resource", "deployment", "2024", null, entraId, input, user);
        var httpRequest = request.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(
            httpPost.getURI().toString(),
            is("https://resource.openai.azure.com/openai/deployments/deployment/embeddings?api-version=2024")
        );

        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer " + entraId));

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap.size(), equalTo(2));
        assertThat(requestMap.get("input"), is(List.of(input)));
        assertThat(requestMap.get("user"), is(user));
    }

    public void testTruncate_ReducesInputTextSizeByHalf() throws IOException {
        var request = createRequest("resource", "deployment", "apiVersion", "apikey", null, "abcd", null);
        var truncatedRequest = request.truncate();

        var httpRequest = truncatedRequest.createHttpRequest();
        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();
        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(1));
        assertThat(requestMap.get("input"), is(List.of("ab")));
    }

    public void testIsTruncated_ReturnsTrue() {
        var request = createRequest("resource", "deployment", "apiVersion", "apikey", null, "abcd", null);
        assertFalse(request.getTruncationInfo()[0]);

        var truncatedRequest = request.truncate();
        assertTrue(truncatedRequest.getTruncationInfo()[0]);
    }

    public AzureOpenAiEmbeddingsRequest createRequest(
        String resourceName,
        String deploymentId,
        String apiVersion,
        @Nullable String apiKey,
        @Nullable String entraId,
        String input,
        @Nullable String user
    ) {
        var embeddingsModel = AzureOpenAiEmbeddingsModelTests.createModel(
            resourceName,
            deploymentId,
            apiVersion,
            user,
            apiKey,
            entraId,
            "id"
        );
        return new AzureOpenAiEmbeddingsRequest(
            TruncatorTests.createTruncator(),
            new Truncator.TruncationResult(List.of(input), new boolean[] { false }),
            embeddingsModel
        );
    }
}
