/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.response;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class OpenAiUnifiedChatCompletionResponseEntityTests extends ESTestCase {

    public void testFromResponse_FullResponse_PreservesAllFields() throws IOException {
        var json = """
            {
              "id": "chatcmpl-abc123",
              "object": "chat.completion",
              "model": "gpt-4o",
              "choices": [
                {
                  "index": 0,
                  "message": {
                    "role": "assistant",
                    "content": "Hello, world!",
                    "tool_calls": [
                      {
                        "id": "call_xyz",
                        "type": "function",
                        "function": {
                          "name": "get_weather",
                          "arguments": "{\\"city\\": \\"London\\"}"
                        }
                      }
                    ],
                    "refusal": null
                  },
                  "finish_reason": "tool_calls"
                }
              ],
              "usage": {
                "prompt_tokens": 9,
                "completion_tokens": 12,
                "total_tokens": 21
              }
            }
            """;

        var result = OpenAiUnifiedChatCompletionResponseEntity.fromResponse(json.getBytes(StandardCharsets.UTF_8));

        assertThat(result.id(), is("chatcmpl-abc123"));
        assertThat(result.object(), is("chat.completion"));
        assertThat(result.model(), is("gpt-4o"));
        assertThat(result.choices(), hasSize(1));

        var choice = result.choices().get(0);
        assertThat(choice.index(), is(0));
        assertThat(choice.finishReason(), is("tool_calls"));

        var message = choice.message();
        assertThat(message.role(), is("assistant"));
        assertThat(message.content(), is("Hello, world!"));
        assertThat(message.refusal(), nullValue());
        assertThat(message.toolCalls(), notNullValue());
        assertThat(message.toolCalls(), hasSize(1));

        var toolCall = message.toolCalls().get(0);
        assertThat(toolCall.id(), is("call_xyz"));
        assertThat(toolCall.type(), is("function"));
        assertThat(toolCall.function().name(), is("get_weather"));
        assertThat(toolCall.function().arguments(), is("{\"city\": \"London\"}"));

        var usage = result.usage();
        assertThat(usage, notNullValue());
        assertThat(usage.promptTokens(), is(9));
        assertThat(usage.completionTokens(), is(12));
        assertThat(usage.totalTokens(), is(21));
    }

    public void testFromResponse_MinimalResponse_ParsesSuccessfully() throws IOException {
        var json = """
            {
              "id": "chatcmpl-minimal",
              "object": "chat.completion",
              "model": "gpt-4o-mini",
              "choices": [
                {
                  "index": 0,
                  "message": {
                    "role": "assistant",
                    "content": "Hi"
                  },
                  "finish_reason": "stop"
                }
              ]
            }
            """;

        var result = OpenAiUnifiedChatCompletionResponseEntity.fromResponse(json.getBytes(StandardCharsets.UTF_8));

        assertThat(result.id(), is("chatcmpl-minimal"));
        assertThat(result.model(), is("gpt-4o-mini"));
        assertThat(result.usage(), nullValue());
        assertThat(result.choices(), hasSize(1));

        var message = result.choices().get(0).message();
        assertThat(message.role(), is("assistant"));
        assertThat(message.content(), is("Hi"));
        assertThat(message.toolCalls(), nullValue());
        assertThat(message.refusal(), nullValue());
    }

    public void testFromResponse_MultipleChoices_PreservesAll() throws IOException {
        var json = """
            {
              "id": "chatcmpl-multi",
              "object": "chat.completion",
              "model": "gpt-4o",
              "choices": [
                {
                  "index": 0,
                  "message": {
                    "role": "assistant",
                    "content": "First choice"
                  },
                  "finish_reason": "stop"
                },
                {
                  "index": 1,
                  "message": {
                    "role": "assistant",
                    "content": "Second choice"
                  },
                  "finish_reason": "stop"
                }
              ]
            }
            """;

        var result = OpenAiUnifiedChatCompletionResponseEntity.fromResponse(json.getBytes(StandardCharsets.UTF_8));

        assertThat(result.choices(), hasSize(2));
        assertThat(result.choices().get(0).message().content(), is("First choice"));
        assertThat(result.choices().get(1).message().content(), is("Second choice"));
    }

    public void testFromResponse_WithRefusal_ParsesRefusal() throws IOException {
        var json = """
            {
              "id": "chatcmpl-refusal",
              "object": "chat.completion",
              "model": "gpt-4o",
              "choices": [
                {
                  "index": 0,
                  "message": {
                    "role": "assistant",
                    "content": null,
                    "refusal": "I cannot help with that."
                  },
                  "finish_reason": "stop"
                }
              ]
            }
            """;

        var result = OpenAiUnifiedChatCompletionResponseEntity.fromResponse(json.getBytes(StandardCharsets.UTF_8));

        var message = result.choices().get(0).message();
        assertThat(message.content(), nullValue());
        assertThat(message.refusal(), is("I cannot help with that."));
    }

    public void testFromResponse_InvalidJson_ThrowsException() {
        var invalidJson = "not valid json".getBytes(StandardCharsets.UTF_8);
        expectThrows(Exception.class, () -> OpenAiUnifiedChatCompletionResponseEntity.fromResponse(invalidJson));
    }

    public void testFromResponse_MissingRequiredField_ThrowsException() {
        var json = """
            {
              "object": "chat.completion",
              "model": "gpt-4o",
              "choices": []
            }
            """;
        // Missing "id" field which is a required constructor arg
        expectThrows(Exception.class, () -> OpenAiUnifiedChatCompletionResponseEntity.fromResponse(json.getBytes(StandardCharsets.UTF_8)));
    }
}
