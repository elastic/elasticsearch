/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.response;

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

public class CohereCompletionResponseEntityTests extends ESTestCase {

    public void testFromResponse_CreatesResponseEntityForText() throws IOException {
        String responseJson = """
            {
                "response_id": "some id",
                "text": "result",
                "generation_id": "some id",
                "chat_history": [
                    {
                        "role": "USER",
                        "message": "some input"
                    },
                    {
                        "role": "CHATBOT",
                        "message": "result"
                    }
                ],
                "finish_reason": "COMPLETE",
                "meta": {
                    "api_version": {
                        "version": "1"
                    },
                    "billed_units": {
                        "input_tokens": 4,
                        "output_tokens": 191
                    },
                    "tokens": {
                        "input_tokens": 70,
                        "output_tokens": 191
                    }
                }
            }
            """;

        ChatCompletionResults chatCompletionResults = CohereCompletionResponseEntity.fromResponse(
            mock(Request.class),
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(chatCompletionResults.getResults().size(), is(1));
        assertThat(chatCompletionResults.getResults().get(0).content(), is("result"));
    }

    public void testFromResponse_FailsWhenTextIsNotPresent() {
        String responseJson = """
            {
                "response_id": "some id",
                "not_text": "result",
                "generation_id": "some id",
                "chat_history": [
                    {
                        "role": "USER",
                        "message": "some input"
                    },
                    {
                        "role": "CHATBOT",
                        "message": "result"
                    }
                ],
                "finish_reason": "COMPLETE",
                "meta": {
                    "api_version": {
                        "version": "1"
                    },
                    "billed_units": {
                        "input_tokens": 4,
                        "output_tokens": 191
                    },
                    "tokens": {
                        "input_tokens": 70,
                        "output_tokens": 191
                    }
                }
            }
            """;

        var thrownException = expectThrows(
            IllegalStateException.class,
            () -> CohereCompletionResponseEntity.fromResponse(
                mock(Request.class),
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(thrownException.getMessage(), is("Failed to find required field [text] in Cohere chat response"));
    }

    public void testFromResponse_FailsWhenTextIsNotAString() {
        String responseJson = """
            {
                "response_id": "some id",
                "text": {
                    "text": "result"
                },
                "generation_id": "some id",
                "chat_history": [
                    {
                        "role": "USER",
                        "message": "some input"
                    },
                    {
                        "role": "CHATBOT",
                        "message": "result"
                    }
                ],
                "finish_reason": "COMPLETE",
                "meta": {
                    "api_version": {
                        "version": "1"
                    },
                    "billed_units": {
                        "input_tokens": 4,
                        "output_tokens": 191
                    },
                    "tokens": {
                        "input_tokens": 70,
                        "output_tokens": 191
                    }
                }
            }
            """;

        var thrownException = expectThrows(
            ParsingException.class,
            () -> CohereCompletionResponseEntity.fromResponse(
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
