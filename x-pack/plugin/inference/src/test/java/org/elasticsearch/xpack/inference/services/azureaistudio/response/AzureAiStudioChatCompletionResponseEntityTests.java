/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaistudio.response;

import org.apache.http.HttpResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioEndpointType;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioProvider;
import org.elasticsearch.xpack.inference.services.azureaistudio.completion.AzureAiStudioChatCompletionModelTests;
import org.elasticsearch.xpack.inference.services.azureaistudio.request.AzureAiStudioChatCompletionRequest;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class AzureAiStudioChatCompletionResponseEntityTests extends ESTestCase {

    public void testCompletionResponse_FromTokenEndpoint() throws IOException {
        var entity = new AzureAiStudioChatCompletionResponseEntity();
        var model = AzureAiStudioChatCompletionModelTests.createModel(
            "id",
            "http://testopenai.local",
            AzureAiStudioProvider.OPENAI,
            AzureAiStudioEndpointType.TOKEN,
            "apikey"
        );
        var request = new AzureAiStudioChatCompletionRequest(model, List.of("test input"), false);
        var result = (ChatCompletionResults) entity.apply(
            request,
            new HttpResult(mock(HttpResponse.class), testTokenResponseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(result.getResults().size(), equalTo(1));
        assertThat(result.getResults().get(0).content(), is("test input string"));
    }

    public void testCompletionResponse_FromRealtimeEndpoint() throws IOException {
        var entity = new AzureAiStudioChatCompletionResponseEntity();
        var model = AzureAiStudioChatCompletionModelTests.createModel(
            "id",
            "http://testmistral.local",
            AzureAiStudioProvider.MISTRAL,
            AzureAiStudioEndpointType.REALTIME,
            "apikey"
        );
        var request = new AzureAiStudioChatCompletionRequest(model, List.of("test input"), false);
        var result = (ChatCompletionResults) entity.apply(
            request,
            new HttpResult(mock(HttpResponse.class), testRealtimeResponseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(result.getResults().size(), equalTo(1));
        assertThat(result.getResults().get(0).content(), is("test realtime response"));
    }

    private static String testRealtimeResponseJson = """
        {
            "output": "test realtime response"
        }
        """;

    private static String testTokenResponseJson = """
        {
            "choices": [
                {
                    "finish_reason": "stop",
                    "index": 0,
                    "message": {
                        "content": "test input string",
                        "role": "assistant",
                        "tool_calls": null
                    }
                }
            ],
            "created": 1714006424,
            "id": "f92b5b4d-0de3-4152-a3c6-5aae8a74555c",
            "model": "",
            "object": "chat.completion",
            "usage": {
                "completion_tokens": 35,
                "prompt_tokens": 8,
                "total_tokens": 43
            }
        }""";
}
