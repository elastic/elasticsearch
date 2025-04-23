/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.request.completion;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.azureopenai.completion.AzureOpenAiCompletionModelTests;
import org.elasticsearch.xpack.inference.services.azureopenai.request.AzureOpenAiCompletionRequest;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.services.azureopenai.action.AzureOpenAiActionCreatorTests.getContentOfMessageInRequestMap;
import static org.elasticsearch.xpack.inference.services.azureopenai.request.AzureOpenAiUtils.API_KEY_HEADER;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class AzureOpenAiCompletionRequestTests extends ESTestCase {

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
            is("https://resource.openai.azure.com/openai/deployments/deployment/chat/completions?api-version=2024")
        );

        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        assertThat(httpPost.getLastHeader(API_KEY_HEADER).getValue(), is(apiKey));

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(getContentOfMessageInRequestMap(requestMap), is(input));
        assertThat(requestMap.get("user"), is(user));
        assertThat(requestMap.get("n"), is(1));
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
            is("https://resource.openai.azure.com/openai/deployments/deployment/chat/completions?api-version=2024")
        );

        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer " + entraId));

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(getContentOfMessageInRequestMap(requestMap), is(input));
        assertThat(requestMap.get("user"), is(user));
        assertThat(requestMap.get("n"), is(1));
    }

    protected AzureOpenAiCompletionRequest createRequest(
        String resource,
        String deployment,
        String apiVersion,
        String apiKey,
        String entraId,
        String input,
        String user
    ) {
        var completionModel = AzureOpenAiCompletionModelTests.createCompletionModel(
            resource,
            deployment,
            apiVersion,
            user,
            apiKey,
            entraId,
            "id"
        );

        return new AzureOpenAiCompletionRequest(List.of(input), completionModel, false);
    }

}
