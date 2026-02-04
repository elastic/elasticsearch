/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.anthropic.response;

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

public class AnthropicChatCompletionResponseEntityTests extends ESTestCase {

    public void testFromResponse_CreatesResultsForASingleItem() throws IOException {
        String responseJson = """
            {
                "id": "msg_01XzZQmG41BMGe5NZ5p2vEWb",
                "type": "message",
                "role": "assistant",
                "model": "claude-3-opus-20240229",
                "content": [
                    {
                        "type": "text",
                        "text": "result"
                    }
                ],
                "stop_reason": "end_turn",
                "stop_sequence": null,
                "usage": {
                    "input_tokens": 16,
                    "output_tokens": 326
                }
            }
            """;

        ChatCompletionResults chatCompletionResults = AnthropicChatCompletionResponseEntity.fromResponse(
            mock(Request.class),
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(chatCompletionResults.getResults().size(), is(1));
        assertThat(chatCompletionResults.getResults().get(0).content(), is("result"));
    }

    public void testFromResponse_CreatesResultsForMultipleItems() throws IOException {
        String responseJson = """
            {
                "id": "msg_01XzZQmG41BMGe5NZ5p2vEWb",
                "type": "message",
                "role": "assistant",
                "model": "claude-3-opus-20240229",
                "content": [
                    {
                        "type": "text",
                        "text": "result"
                    },
                    {
                        "type": "text",
                        "text": "result2"
                    }
                ],
                "stop_reason": "end_turn",
                "stop_sequence": null,
                "usage": {
                    "input_tokens": 16,
                    "output_tokens": 326
                }
            }
            """;

        ChatCompletionResults chatCompletionResults = AnthropicChatCompletionResponseEntity.fromResponse(
            mock(Request.class),
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(chatCompletionResults.getResults().size(), is(2));
        assertThat(chatCompletionResults.getResults().get(0).content(), is("result"));
        assertThat(chatCompletionResults.getResults().get(1).content(), is("result2"));
    }

    public void testFromResponse_CreatesResultsForMultipleItems_IgnoresTools() throws IOException {
        String responseJson = """
            {
                "id": "msg_01XzZQmG41BMGe5NZ5p2vEWb",
                "type": "message",
                "role": "assistant",
                "model": "claude-3-opus-20240229",
                "content": [
                    {
                        "type": "text",
                        "text": "result"
                    },
                    {
                        "type": "tool_use",
                        "id": "toolu_01Dc8BGR8aEuToS2B9uz6HMX",
                        "name": "get_weather",
                        "input": {
                            "location": "San Francisco, CA"
                        }
                    },
                    {
                        "type": "text",
                        "text": "result2"
                    }
                ],
                "stop_reason": "end_turn",
                "stop_sequence": null,
                "usage": {
                    "input_tokens": 16,
                    "output_tokens": 326
                }
            }
            """;

        ChatCompletionResults chatCompletionResults = AnthropicChatCompletionResponseEntity.fromResponse(
            mock(Request.class),
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(chatCompletionResults.getResults().size(), is(2));
        assertThat(chatCompletionResults.getResults().get(0).content(), is("result"));
        assertThat(chatCompletionResults.getResults().get(1).content(), is("result2"));
    }

    public void testFromResponse_FailsWhenContentIsNotPresent() {
        String responseJson = """
            {
                "id": "msg_01XzZQmG41BMGe5NZ5p2vEWb",
                "type": "message",
                "role": "assistant",
                "model": "claude-3-opus-20240229",
                "not_content": [
                    {
                        "type": "text",
                        "text": "result"
                    }
                ],
                "stop_reason": "end_turn",
                "stop_sequence": null,
                "usage": {
                    "input_tokens": 16,
                    "output_tokens": 326
                }
            }
            """;

        var thrownException = expectThrows(
            IllegalStateException.class,
            () -> AnthropicChatCompletionResponseEntity.fromResponse(
                mock(Request.class),
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(thrownException.getMessage(), is("Failed to find required field [content] in Anthropic chat completions response"));
    }

    public void testFromResponse_FailsWhenContentFieldNotAnArray() {
        String responseJson = """
            {
                "id": "msg_01XzZQmG41BMGe5NZ5p2vEWb",
                "type": "message",
                "role": "assistant",
                "model": "claude-3-opus-20240229",
                "content": {
                        "type": "text",
                        "text": "result"
                },
                "stop_reason": "end_turn",
                "stop_sequence": null,
                "usage": {
                    "input_tokens": 16,
                    "output_tokens": 326
                }
            }
            """;

        var thrownException = expectThrows(
            ParsingException.class,
            () -> AnthropicChatCompletionResponseEntity.fromResponse(
                mock(Request.class),
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(
            thrownException.getMessage(),
            is("Failed to parse object: expecting token of type [START_ARRAY] but found [START_OBJECT]")
        );
    }

    public void testFromResponse_FailsWhenTypeDoesNotExist() {
        String responseJson = """
            {
                "id": "msg_01XzZQmG41BMGe5NZ5p2vEWb",
                "type": "message",
                "role": "assistant",
                "model": "claude-3-opus-20240229",
                "not_content": [
                    {
                        "text": "result"
                    }
                ],
                "stop_reason": "end_turn",
                "stop_sequence": null,
                "usage": {
                    "input_tokens": 16,
                    "output_tokens": 326
                }
            }
            """;

        var thrownException = expectThrows(
            IllegalStateException.class,
            () -> AnthropicChatCompletionResponseEntity.fromResponse(
                mock(Request.class),
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(thrownException.getMessage(), is("Failed to find required field [content] in Anthropic chat completions response"));
    }

    public void testFromResponse_FailsWhenContentValueIsAString() {
        String responseJson = """
            {
                "id": "msg_01XzZQmG41BMGe5NZ5p2vEWb",
                "type": "message",
                "role": "assistant",
                "model": "claude-3-opus-20240229",
                "content": "hello",
                "stop_reason": "end_turn",
                "stop_sequence": null,
                "usage": {
                    "input_tokens": 16,
                    "output_tokens": 326
                }
            }
            """;

        var thrownException = expectThrows(
            ParsingException.class,
            () -> AnthropicChatCompletionResponseEntity.fromResponse(
                mock(Request.class),
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(
            thrownException.getMessage(),
            is("Failed to parse object: expecting token of type [START_ARRAY] but found [VALUE_STRING]")
        );
    }
}
