/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.response;

import org.apache.http.HttpResponse;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class GoogleVertexAiCompletionResponseEntityTests extends ESTestCase {

    public void testFromResponse_Javadoc() throws IOException {
        var responseText = "I am sorry, I cannot summarize the text because I do not have access to the text you are referring to.";

        String responseJson = Strings.format("""
              {
              "candidates": [
                {
                  "content": {
                    "role": "model",
                    "parts": [
                      {
                        "text": "%s"
                      }
                    ]
                  },
                  "finishReason": "STOP",
                  "avgLogprobs": -0.19326641248620074
                }
              ],
              "usageMetadata": {
                "promptTokenCount": 71,
                "candidatesTokenCount": 23,
                "totalTokenCount": 94,
                "trafficType": "ON_DEMAND",
                "promptTokensDetails": [
                  {
                    "modality": "TEXT",
                    "tokenCount": 71
                  }
                ],
                "candidatesTokensDetails": [
                  {
                    "modality": "TEXT",
                    "tokenCount": 23
                  }
                ]
              },
              "modelVersion": "gemini-2.0-flash-001",
              "createTime": "2025-05-28T15:08:20.049493Z",
              "responseId": "5CY3aNWCA6mm4_UPr-zduAE"
            }
            """, responseText);

        var parsedResults = GoogleVertexAiCompletionResponseEntity.fromResponse(
            mock(Request.class),
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assert parsedResults instanceof ChatCompletionResults;
        var results = (ChatCompletionResults) parsedResults;

        assertThat(results.isStreaming(), is(false));
        assertThat(results.results().size(), is(1));
        assertThat(results.results().get(0).content(), is(responseText));
    }
}
