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
        assertNull(message.refusal());
        assertNotNull(message.toolCalls());
        assertThat(message.toolCalls(), hasSize(1));

        var toolCall = message.toolCalls().get(0);
        assertThat(toolCall.index(), is(0));
        assertThat(toolCall.id(), is("call_xyz"));
        assertThat(toolCall.type(), is("function"));
        assertThat(toolCall.function().name(), is("get_weather"));
        assertThat(toolCall.function().arguments(), is("{\"city\": \"London\"}"));

        var usage = result.usage();
        assertNotNull(usage);
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
        assertNull(result.usage());
        assertThat(result.choices(), hasSize(1));

        var message = result.choices().get(0).message();
        assertThat(message.role(), is("assistant"));
        assertThat(message.content(), is("Hi"));
        assertNull(message.toolCalls());
        assertNull(message.refusal());
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
        assertNull(message.content());
        assertThat(message.refusal(), is("I cannot help with that."));
    }

    public void testFromResponse_ToolCallsWithoutIndex_AssignsPositionalIndex() throws IOException {
        var json = """
            {
              "id": "chatcmpl-tc1",
              "object": "chat.completion",
              "model": "gpt-4o",
              "choices": [
                {
                  "index": 0,
                  "message": {
                    "role": "assistant",
                    "content": null,
                    "tool_calls": [
                      {
                        "id": "call_a",
                        "type": "function",
                        "function": { "name": "fn_a", "arguments": "{}" }
                      },
                      {
                        "id": "call_b",
                        "type": "function",
                        "function": { "name": "fn_b", "arguments": "{}" }
                      },
                      {
                        "id": "call_c",
                        "type": "function",
                        "function": { "name": "fn_c", "arguments": "{}" }
                      }
                    ]
                  },
                  "finish_reason": "tool_calls"
                }
              ]
            }
            """;

        var result = OpenAiUnifiedChatCompletionResponseEntity.fromResponse(json.getBytes(StandardCharsets.UTF_8));
        var toolCalls = result.choices().get(0).message().toolCalls();
        assertNotNull(toolCalls);
        assertThat(toolCalls, hasSize(3));
        assertThat(toolCalls.get(0).index(), is(0));
        assertThat(toolCalls.get(1).index(), is(1));
        assertThat(toolCalls.get(2).index(), is(2));
    }

    public void testFromResponse_MultipleChoices_ResetsToolCallIndexPerChoice() throws IOException {
        var json = """
            {
              "id": "chatcmpl-mc1",
              "object": "chat.completion",
              "model": "gpt-4o",
              "choices": [
                {
                  "index": 0,
                  "message": {
                    "role": "assistant",
                    "content": null,
                    "tool_calls": [
                      {
                        "id": "call_a0",
                        "type": "function",
                        "function": { "name": "fn_a", "arguments": "{}" }
                      },
                      {
                        "id": "call_a1",
                        "type": "function",
                        "function": { "name": "fn_b", "arguments": "{}" }
                      }
                    ]
                  },
                  "finish_reason": "tool_calls"
                },
                {
                  "index": 1,
                  "message": {
                    "role": "assistant",
                    "content": null,
                    "tool_calls": [
                      {
                        "id": "call_b0",
                        "type": "function",
                        "function": { "name": "fn_c", "arguments": "{}" }
                      },
                      {
                        "id": "call_b1",
                        "type": "function",
                        "function": { "name": "fn_d", "arguments": "{}" }
                      }
                    ]
                  },
                  "finish_reason": "tool_calls"
                }
              ]
            }
            """;

        var result = OpenAiUnifiedChatCompletionResponseEntity.fromResponse(json.getBytes(StandardCharsets.UTF_8));
        assertThat(result.choices(), hasSize(2));

        var firstToolCalls = result.choices().get(0).message().toolCalls();
        assertNotNull(firstToolCalls);
        assertThat(firstToolCalls, hasSize(2));
        assertThat(firstToolCalls.get(0).index(), is(0));
        assertThat(firstToolCalls.get(1).index(), is(1));

        var secondToolCalls = result.choices().get(1).message().toolCalls();
        assertNotNull(secondToolCalls);
        assertThat(secondToolCalls, hasSize(2));
        assertThat(secondToolCalls.get(0).index(), is(0));
        assertThat(secondToolCalls.get(1).index(), is(1));
    }

    public void testFromResponse_ToolCallsWithExplicitIndex_HonorsDeclaredValues() throws IOException {
        var json = """
            {
              "id": "chatcmpl-ei1",
              "object": "chat.completion",
              "model": "gpt-4o",
              "choices": [
                {
                  "index": 0,
                  "message": {
                    "role": "assistant",
                    "content": null,
                    "tool_calls": [
                      {
                        "index": 1,
                        "id": "call_a",
                        "type": "function",
                        "function": { "name": "fn_a", "arguments": "{}" }
                      },
                      {
                        "index": 0,
                        "id": "call_b",
                        "type": "function",
                        "function": { "name": "fn_b", "arguments": "{}" }
                      }
                    ]
                  },
                  "finish_reason": "tool_calls"
                }
              ]
            }
            """;

        var result = OpenAiUnifiedChatCompletionResponseEntity.fromResponse(json.getBytes(StandardCharsets.UTF_8));
        var toolCalls = result.choices().get(0).message().toolCalls();
        assertNotNull(toolCalls);
        assertThat(toolCalls, hasSize(2));
        assertThat(toolCalls.get(0).index(), is(1));
        assertThat(toolCalls.get(1).index(), is(0));
    }

    public void testFromResponse_ToolCallsWithMixedIndex_ThrowsException() {
        var json = """
            {
              "id": "chatcmpl-mix1",
              "object": "chat.completion",
              "model": "gpt-4o",
              "choices": [
                {
                  "index": 0,
                  "message": {
                    "role": "assistant",
                    "content": null,
                    "tool_calls": [
                      {
                        "index": 0,
                        "id": "call_a",
                        "type": "function",
                        "function": { "name": "fn_a", "arguments": "{}" }
                      },
                      {
                        "id": "call_b",
                        "type": "function",
                        "function": { "name": "fn_b", "arguments": "{}" }
                      }
                    ]
                  },
                  "finish_reason": "tool_calls"
                }
              ]
            }
            """;

        expectThrows(Exception.class, () -> OpenAiUnifiedChatCompletionResponseEntity.fromResponse(json.getBytes(StandardCharsets.UTF_8)));
    }

    public void testFromResponse_NullToolCalls_ParsesAsNull() throws IOException {
        var json = """
            {
              "id": "chatcmpl-ntc1",
              "object": "chat.completion",
              "model": "gpt-4o",
              "choices": [
                {
                  "index": 0,
                  "message": {
                    "role": "assistant",
                    "content": "Hello",
                    "tool_calls": null
                  },
                  "finish_reason": "stop"
                }
              ]
            }
            """;

        var result = OpenAiUnifiedChatCompletionResponseEntity.fromResponse(json.getBytes(StandardCharsets.UTF_8));
        assertNull(result.choices().get(0).message().toolCalls());
    }

    public void testFromResponse_NullPromptTokensDetails_ParsesSuccessfully() throws IOException {
        var json = """
            {
              "id": "chatcmpl-ptd1",
              "object": "chat.completion",
              "model": "gpt-4o",
              "choices": [
                {
                  "index": 0,
                  "message": {
                    "role": "assistant",
                    "content": "Hello"
                  },
                  "finish_reason": "stop"
                }
              ],
              "usage": {
                "prompt_tokens": 5,
                "completion_tokens": 10,
                "total_tokens": 15,
                "prompt_tokens_details": null
              }
            }
            """;

        var result = OpenAiUnifiedChatCompletionResponseEntity.fromResponse(json.getBytes(StandardCharsets.UTF_8));
        assertNotNull(result.usage());
        assertThat(result.usage().promptTokens(), is(5));
        assertThat(result.usage().completionTokens(), is(10));
        assertThat(result.usage().totalTokens(), is(15));
        assertNull(result.usage().promptTokensDetails());
    }

    public void testFromResponse_EmptyPromptTokensDetails_ParsesToNull() throws IOException {
        var json = """
            {
              "id": "chatcmpl-ptd2",
              "object": "chat.completion",
              "model": "gpt-4o",
              "choices": [
                {
                  "index": 0,
                  "message": {
                    "role": "assistant",
                    "content": "Hello"
                  },
                  "finish_reason": "stop"
                }
              ],
              "usage": {
                "prompt_tokens": 5,
                "completion_tokens": 10,
                "total_tokens": 15,
                "prompt_tokens_details": {}
              }
            }
            """;

        var result = OpenAiUnifiedChatCompletionResponseEntity.fromResponse(json.getBytes(StandardCharsets.UTF_8));
        assertNotNull(result.usage());
        assertNull(result.usage().promptTokensDetails());
    }

    public void testFromResponse_ExplicitNullTokenCounts_ParsesToNull() throws IOException {
        // A provider sending explicit JSON nulls for the count fields should not fail the parse,
        // and the detail objects should collapse to null (both fields null → ofNullable returns null).
        var json = """
            {
              "id": "chatcmpl-ptd3",
              "object": "chat.completion",
              "model": "gpt-4o",
              "choices": [
                {
                  "index": 0,
                  "message": {
                    "role": "assistant",
                    "content": "Hello"
                  },
                  "finish_reason": "stop"
                }
              ],
              "usage": {
                "prompt_tokens": 5,
                "completion_tokens": 10,
                "total_tokens": 15,
                "prompt_tokens_details": {
                  "cached_tokens": null,
                  "cache_write_tokens": null
                },
                "completion_tokens_details": {
                  "reasoning_tokens": null
                }
              }
            }
            """;

        var result = OpenAiUnifiedChatCompletionResponseEntity.fromResponse(json.getBytes(StandardCharsets.UTF_8));
        assertNotNull(result.usage());
        assertNull(result.usage().promptTokensDetails());
        assertNull(result.usage().completionTokenDetails());
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
