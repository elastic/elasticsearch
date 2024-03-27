/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.openai;

import org.apache.http.HttpResponse;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class OpenAiChatCompletionResponseEntityTests extends ESTestCase {

    public void testFromResponse_CreatesResultsForASingleItem() throws IOException {
        String responseJson = """
            {
              "id": "some-id",
              "object": "chat.completion",
              "created": 1705397787,
              "model": "gpt-3.5-turbo-0613",
              "choices": [
                {
                  "index": 0,
                  "message": {
                    "role": "assistant",
                    "content": "some content"
                  },
                  "logprobs": null,
                  "finish_reason": "stop"
                }
              ],
              "usage": {
                "prompt_tokens": 46,
                "completion_tokens": 39,
                "total_tokens": 85
              },
              "system_fingerprint": null
            }
            """;

        ChatCompletionResults chatCompletionResults = OpenAiChatCompletionResponseEntity.fromResponse(
            mock(Request.class),
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(chatCompletionResults.getResults().size(), equalTo(1));
    }

    public void testFromResponse_FailsWhenChoicesFieldIsNotPresent() {
        String responseJson = """
            {
              "id": "some-id",
              "object": "chat.completion",
              "created": 1705397787,
              "model": "gpt-3.5-turbo-0613",
              "not_choices": [
                {
                  "index": 0,
                  "message": {
                    "role": "assistant",
                    "content": "some content"
                  },
                  "logprobs": null,
                  "finish_reason": "stop"
                },
              ],
              "usage": {
                "prompt_tokens": 46,
                "completion_tokens": 39,
                "total_tokens": 85
              },
              "system_fingerprint": null
            }
            """;

        var thrownException = expectThrows(
            IllegalStateException.class,
            () -> OpenAiChatCompletionResponseEntity.fromResponse(
                mock(Request.class),
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(thrownException.getMessage(), is("Failed to find required field [choices] in OpenAI chat completions response"));
    }

    public void testFromResponse_FailsWhenChoicesFieldNotAnArray() {
        String responseJson = """
             {
              "id": "some-id",
              "object": "chat.completion",
              "created": 1705397787,
              "model": "gpt-3.5-turbo-0613",
              "choices": {
                "test": {
                  "index": 0,
                  "message": {
                    "role": "assistant",
                    "content": "some content"
                  },
                  "logprobs": null,
                  "finish_reason": "stop"
                },
              },
              "usage": {
                "prompt_tokens": 46,
                "completion_tokens": 39,
                "total_tokens": 85
              },
              "system_fingerprint": null
            }
            """;

        var thrownException = expectThrows(
            ParsingException.class,
            () -> OpenAiChatCompletionResponseEntity.fromResponse(
                mock(Request.class),
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(
            thrownException.getMessage(),
            is("Failed to parse object: expecting token of type [START_OBJECT] but found [FIELD_NAME]")
        );
    }

    public void testFromResponse_FailsWhenMessageDoesNotExist() {
        String responseJson = """
             {
              "id": "some-id",
              "object": "chat.completion",
              "created": 1705397787,
              "model": "gpt-3.5-turbo-0613",
              "choices": [
                {
                  "index": 0,
                  "not_message": {
                    "role": "assistant",
                    "content": "some content"
                  },
                  "logprobs": null,
                  "finish_reason": "stop"
                },
              ],
              "usage": {
                "prompt_tokens": 46,
                "completion_tokens": 39,
                "total_tokens": 85
              },
              "system_fingerprint": null
            }
            """;

        var thrownException = expectThrows(
            IllegalStateException.class,
            () -> OpenAiChatCompletionResponseEntity.fromResponse(
                mock(Request.class),
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(thrownException.getMessage(), is("Failed to find required field [message] in OpenAI chat completions response"));
    }

    public void testFromResponse_FailsWhenMessageValueIsAString() {
        String responseJson = """
             {
              "id": "some-id",
              "object": "chat.completion",
              "created": 1705397787,
              "model": "gpt-3.5-turbo-0613",
              "choices": [
                {
                  "index": 0,
                  "message": "some content",
                  "logprobs": null,
                  "finish_reason": "stop"
                },
              ],
              "usage": {
                "prompt_tokens": 46,
                "completion_tokens": 39,
                "total_tokens": 85
              },
              "system_fingerprint": null
            }
            """;

        var thrownException = expectThrows(
            ParsingException.class,
            () -> OpenAiChatCompletionResponseEntity.fromResponse(
                mock(Request.class),
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(
            thrownException.getMessage(),
            is("Failed to parse object: expecting token of type [START_OBJECT] but found [VALUE_STRING]")
        );
    }
}
