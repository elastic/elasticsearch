/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * this file was contributed to by a generative AI
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

public class StreamingUnifiedChatCompletionResultsTests extends ESTestCase {

    public void testResults_toXContentChunked() throws IOException {
        String expected = """
                        {
                          "id": "chunk1",
                          "choices": [
                            {
                              "delta": {
                                "content": "example_content",
                                "refusal": "example_refusal",
                                "role": "assistant",
                                "tool_calls": [
                                  {
                                    "index": 1,
                                    "id": "tool1",
                                    "function": {
                                      "arguments": "example_arguments",
                                      "name": "example_function"
                                    },
                                    "type": "function"
                                  }
                                ]
                              },
                              "finish_reason": "example_reason",
                              "index": 0
                            }
                          ],
                          "model": "example_model",
                          "object": "example_object",
                          "usage": {
                            "completion_tokens": 10,
                            "prompt_tokens": 5,
                            "total_tokens": 15
                          }
                        }
            """;

        StreamingUnifiedChatCompletionResults.ChatCompletionChunk chunk = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk(
            "chunk1",
            List.of(
                new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice(
                    new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta(
                        "example_content",
                        "example_refusal",
                        "assistant",
                        List.of(
                            new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall(
                                1,
                                "tool1",
                                new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall.Function(
                                    "example_arguments",
                                    "example_function"
                                ),
                                "function"
                            )
                        )
                    ),
                    "example_reason",
                    0
                )
            ),
            "example_model",
            "example_object",
            new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Usage(10, 5, 15)
        );

        Deque<StreamingUnifiedChatCompletionResults.ChatCompletionChunk> deque = new ArrayDeque<>();
        deque.add(chunk);
        StreamingUnifiedChatCompletionResults.Results results = new StreamingUnifiedChatCompletionResults.Results(deque);
        XContentBuilder builder = JsonXContent.contentBuilder();
        results.toXContentChunked(null).forEachRemaining(xContent -> {
            try {
                xContent.toXContent(builder, null);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        assertEquals(expected.replaceAll("\\s+", ""), Strings.toString(builder.prettyPrint()).trim());
    }

    public void testChoiceToXContentChunked() throws IOException {
        String expected = """
            {
              "delta": {
                "content": "example_content",
                "refusal": "example_refusal",
                "role": "assistant",
                "tool_calls": [
                  {
                    "index": 1,
                    "id": "tool1",
                    "function": {
                      "arguments": "example_arguments",
                      "name": "example_function"
                    },
                    "type": "function"
                  }
                ]
              },
              "finish_reason": "example_reason",
              "index": 0
            }
            """;

        StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice choice =
            new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice(
                new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta(
                    "example_content",
                    "example_refusal",
                    "assistant",
                    List.of(
                        new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall(
                            1,
                            "tool1",
                            new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall.Function(
                                "example_arguments",
                                "example_function"
                            ),
                            "function"
                        )
                    )
                ),
                "example_reason",
                0
            );

        XContentBuilder builder = JsonXContent.contentBuilder();
        choice.toXContentChunked(null).forEachRemaining(xContent -> {
            try {
                xContent.toXContent(builder, null);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        assertEquals(expected.replaceAll("\\s+", ""), Strings.toString(builder.prettyPrint()).trim());
    }

    public void testToolCallToXContentChunked() throws IOException {
        String expected = """
            {
              "index": 1,
              "id": "tool1",
              "function": {
                "arguments": "example_arguments",
                "name": "example_function"
              },
              "type": "function"
            }
            """;

        StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall toolCall =
            new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall(
                1,
                "tool1",
                new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall.Function(
                    "example_arguments",
                    "example_function"
                ),
                "function"
            );

        XContentBuilder builder = JsonXContent.contentBuilder();
        toolCall.toXContentChunked(null).forEachRemaining(xContent -> {
            try {
                xContent.toXContent(builder, null);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        assertEquals(expected.replaceAll("\\s+", ""), Strings.toString(builder.prettyPrint()).trim());
    }

}
