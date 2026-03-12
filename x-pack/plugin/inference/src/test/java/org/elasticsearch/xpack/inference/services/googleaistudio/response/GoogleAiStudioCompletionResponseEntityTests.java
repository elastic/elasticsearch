/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googleaistudio.response;

import org.apache.http.HttpResponse;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class GoogleAiStudioCompletionResponseEntityTests extends ESTestCase {

    public void testFromResponse_CreatesResultsForASingleItem() throws IOException {
        String responseJson = """
            {
                "candidates": [
                    {
                        "content": {
                            "parts": [
                                {
                                    "text": "result"
                                }
                            ],
                            "role": "model"
                        },
                        "finishReason": "STOP",
                        "index": 0,
                        "safetyRatings": [
                            {
                                "category": "HARM_CATEGORY_SEXUALLY_EXPLICIT",
                                "probability": "NEGLIGIBLE"
                            },
                            {
                                "category": "HARM_CATEGORY_HATE_SPEECH",
                                "probability": "NEGLIGIBLE"
                            },
                            {
                                "category": "HARM_CATEGORY_HARASSMENT",
                                "probability": "NEGLIGIBLE"
                            },
                            {
                                "category": "HARM_CATEGORY_DANGEROUS_CONTENT",
                                "probability": "NEGLIGIBLE"
                            }
                        ]
                    }
                ],
                "usageMetadata": {
                    "promptTokenCount": 4,
                    "candidatesTokenCount": 312,
                    "totalTokenCount": 316
                }
            }
            """;

        ChatCompletionResults chatCompletionResults = GoogleAiStudioCompletionResponseEntity.fromResponse(
            mock(Request.class),
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(chatCompletionResults.getResults().size(), is(1));
        assertThat(chatCompletionResults.getResults().get(0).content(), is("result"));
    }

    public void testFromResponse_FailsWhenCandidatesFieldIsNotPresent() {
        String responseJson = """
            {
                "not_candidates": [
                    {
                        "content": {
                            "parts": [
                                {
                                    "text": "result"
                                }
                            ],
                            "role": "model"
                        },
                        "finishReason": "STOP",
                        "index": 0,
                        "safetyRatings": [
                            {
                                "category": "HARM_CATEGORY_SEXUALLY_EXPLICIT",
                                "probability": "NEGLIGIBLE"
                            },
                            {
                                "category": "HARM_CATEGORY_HATE_SPEECH",
                                "probability": "NEGLIGIBLE"
                            },
                            {
                                "category": "HARM_CATEGORY_HARASSMENT",
                                "probability": "NEGLIGIBLE"
                            },
                            {
                                "category": "HARM_CATEGORY_DANGEROUS_CONTENT",
                                "probability": "NEGLIGIBLE"
                            }
                        ]
                    }
                ],
                "usageMetadata": {
                    "promptTokenCount": 4,
                    "candidatesTokenCount": 312,
                    "totalTokenCount": 316
                }
            }
            """;

        var thrownException = expectThrows(
            IllegalStateException.class,
            () -> GoogleAiStudioCompletionResponseEntity.fromResponse(
                mock(Request.class),
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(thrownException.getMessage(), is("Failed to find required field [candidates] in Google AI Studio completion response"));
    }

    public void testFromResponse_FailsWhenTextFieldIsNotAString() {
        String responseJson = """
            {
                "candidates": [
                    {
                        "content": {
                            "parts": [
                                {
                                    "text": {
                                        "key": "value"
                                    }
                                }
                            ],
                            "role": "model"
                        },
                        "finishReason": "STOP",
                        "index": 0,
                        "safetyRatings": [
                            {
                                "category": "HARM_CATEGORY_SEXUALLY_EXPLICIT",
                                "probability": "NEGLIGIBLE"
                            },
                            {
                                "category": "HARM_CATEGORY_HATE_SPEECH",
                                "probability": "NEGLIGIBLE"
                            },
                            {
                                "category": "HARM_CATEGORY_HARASSMENT",
                                "probability": "NEGLIGIBLE"
                            },
                            {
                                "category": "HARM_CATEGORY_DANGEROUS_CONTENT",
                                "probability": "NEGLIGIBLE"
                            }
                        ]
                    }
                ],
                "usageMetadata": {
                    "promptTokenCount": 4,
                    "candidatesTokenCount": 312,
                    "totalTokenCount": 316
                }
            }
            """;

        var thrownException = expectThrows(
            ParsingException.class,
            () -> GoogleAiStudioCompletionResponseEntity.fromResponse(
                mock(Request.class),
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(
            thrownException.getMessage(),
            is("Failed to parse object: expecting token of type [VALUE_STRING] but found [START_OBJECT]")
        );
    }
}
