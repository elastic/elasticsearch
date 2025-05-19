/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.response;

import org.apache.http.HttpResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class GoogleVertexAiChatCompletionResponseEntityTests extends ESTestCase {
    public void testFromResponse_CreatesResultsForMultipleChunks() throws IOException {
        String responseJson = """
            [
              {
                "candidates": [
                  {
                    "content": {
                      "role": "model",
                      "parts": [ { "text": "Hello " } ]
                    }
                  }
                ]
              },
              {
                "candidates": [
                  {
                    "content": {
                      "role": "model",
                      "parts": [ { "text": "World" } ]
                    },
                    "finishReason": "STOP"
                  }
                ],
                "usageMetadata": { "promptTokenCount": 5, "candidatesTokenCount": 2, "totalTokenCount": 7 },
                "modelVersion": "gemini-2.0-flash-001",
                "createTime": "2025-04-29T14:32:55.843480Z",
                "responseId": "F-MQaNi9M7OKqsMPmo2aaaa"
              }
            ]
            """;

        ChatCompletionResults chatCompletionResults = GoogleVertexAiChatCompletionResponseEntity.fromResponse(
            mock(Request.class),
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(chatCompletionResults.getResults().size(), is(1));
        assertThat(chatCompletionResults.getResults().getFirst().content(), is("Hello World"));
    }

    public void testFromResponse_FailsWhenChunkMissingCandidates() {
        // Parser ignores unknown fields, but expects 'candidates' for the constructor
        String responseJson = """
            [
              {
                "not_candidates": []
              }
            ]
            """;

        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> GoogleVertexAiChatCompletionResponseEntity.fromResponse(
                mock(Request.class),
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );
        assertThat(thrownException.getMessage(), is("Required [candidates]"));
    }

    public void testFromResponse_FailsWhenCandidateMissingContent() {
        String responseJson = """
            [
              {
                "candidates": [
                  { "not_content": {} }
                ]
              }
            ]
            """;

        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> GoogleVertexAiChatCompletionResponseEntity.fromResponse(
                mock(Request.class),
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );
        assertThat(thrownException.getMessage(), containsString("[Chunk] failed to parse field [candidates]"));
    }

    public void testFromResponse_FailsWhenContentMissingParts() {
        String responseJson = """
            [
              {
                "candidates": [
                  { "content": { "not_parts": [] } }
                ]
              }
            ]
            """;

        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> GoogleVertexAiChatCompletionResponseEntity.fromResponse(
                mock(Request.class),
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );
        assertThat(thrownException.getMessage(), containsString("[Chunk] failed to parse field [candidates]"));
    }

}
