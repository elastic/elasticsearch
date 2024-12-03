/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.openai;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.StreamingUnifiedChatCompletionResults;

import java.io.IOException;
import java.util.List;

public class OpenAiUnifiedStreamingProcessorTests extends ESTestCase {

    public void testJsonLiteral() {
        String json = """
                {
                  "id": "example_id",
                  "choices": [
                    {
                      "delta": {
                        "content": "example_content",
                        "refusal": null,
                        "role": "assistant",
                        "tool_calls": [
                          {
                            "index": 1,
                            "id": "tool_call_id",
                            "function": {
                              "arguments": "example_arguments",
                              "name": "example_function_name"
                            },
                            "type": "function"
                          }
                        ]
                      },
                      "finish_reason": "stop",
                      "index": 0
                    }
                  ],
                  "model": "example_model",
                  "object": "chat.completion.chunk",
                  "usage": {
                    "completion_tokens": 50,
                    "prompt_tokens": 20,
                    "total_tokens": 70
                  }
                }
            """;
        // Parse the JSON
        XContentParserConfiguration parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(
            LoggingDeprecationHandler.INSTANCE
        );
        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, json)) {
            StreamingUnifiedChatCompletionResults.ChatCompletionChunk chunk = OpenAiUnifiedStreamingProcessor.ChatCompletionChunkParser
                .parse(parser);

            // Assertions to verify the parsed object
            assertEquals("example_id", chunk.getId());
            assertEquals("example_model", chunk.getModel());
            assertEquals("chat.completion.chunk", chunk.getObject());
            assertNotNull(chunk.getUsage());
            assertEquals(50, chunk.getUsage().completionTokens());
            assertEquals(20, chunk.getUsage().promptTokens());
            assertEquals(70, chunk.getUsage().totalTokens());

            List<StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice> choices = chunk.getChoices();
            assertEquals(1, choices.size());
            StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice choice = choices.get(0);
            assertEquals("example_content", choice.getDelta().getContent());
            assertNull(choice.getDelta().getRefusal());
            assertEquals("assistant", choice.getDelta().getRole());
            assertEquals("stop", choice.getFinishReason());
            assertEquals(0, choice.getIndex());

            List<StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall> toolCalls = choice.getDelta()
                .getToolCalls();
            assertEquals(1, toolCalls.size());
            StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall toolCall = toolCalls.get(0);
            assertEquals(1, toolCall.getIndex());
            assertEquals("tool_call_id", toolCall.getId());
            assertEquals("example_function_name", toolCall.getFunction().getName());
            assertEquals("example_arguments", toolCall.getFunction().getArguments());
            assertEquals("function", toolCall.getType());
        } catch (IOException e) {
            fail();
        }
    }

    public void testJsonLiteralCornerCases() {
        String json = """
                {
                  "id": "example_id",
                  "choices": [
                    {
                      "delta": {
                        "content": null,
                        "refusal": null,
                        "role": "assistant",
                        "tool_calls": []
                      },
                      "finish_reason": null,
                      "index": 0
                    },
                    {
                      "delta": {
                        "content": "example_content",
                        "refusal": "example_refusal",
                        "role": "user",
                        "tool_calls": [
                          {
                            "index": 1,
                            "function": {
                              "name": "example_function_name"
                            },
                            "type": "function"
                          }
                        ]
                      },
                      "finish_reason": "stop",
                      "index": 1
                    }
                  ],
                  "model": "example_model",
                  "object": "chat.completion.chunk",
                  "usage": {
                    "completion_tokens": 50,
                    "prompt_tokens": 20,
                    "total_tokens": 70
                  }
                }
            """;
        // Parse the JSON
        XContentParserConfiguration parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(
            LoggingDeprecationHandler.INSTANCE
        );
        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, json)) {
            StreamingUnifiedChatCompletionResults.ChatCompletionChunk chunk = OpenAiUnifiedStreamingProcessor.ChatCompletionChunkParser
                .parse(parser);

            // Assertions to verify the parsed object
            assertEquals("example_id", chunk.getId());
            assertEquals("example_model", chunk.getModel());
            assertEquals("chat.completion.chunk", chunk.getObject());
            assertNotNull(chunk.getUsage());
            assertEquals(50, chunk.getUsage().completionTokens());
            assertEquals(20, chunk.getUsage().promptTokens());
            assertEquals(70, chunk.getUsage().totalTokens());

            List<StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice> choices = chunk.getChoices();
            assertEquals(2, choices.size());

            // First choice assertions
            StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice firstChoice = choices.get(0);
            assertNull(firstChoice.getDelta().getContent());
            assertNull(firstChoice.getDelta().getRefusal());
            assertEquals("assistant", firstChoice.getDelta().getRole());
            assertTrue(firstChoice.getDelta().getToolCalls().isEmpty());
            assertNull(firstChoice.getFinishReason());
            assertEquals(0, firstChoice.getIndex());

            // Second choice assertions
            StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice secondChoice = choices.get(1);
            assertEquals("example_content", secondChoice.getDelta().getContent());
            assertEquals("example_refusal", secondChoice.getDelta().getRefusal());
            assertEquals("user", secondChoice.getDelta().getRole());
            assertEquals("stop", secondChoice.getFinishReason());
            assertEquals(1, secondChoice.getIndex());

            List<StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall> toolCalls = secondChoice.getDelta()
                .getToolCalls();
            assertEquals(1, toolCalls.size());
            StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall toolCall = toolCalls.get(0);
            assertEquals(1, toolCall.getIndex());
            assertNull(toolCall.getId());
            assertEquals("example_function_name", toolCall.getFunction().getName());
            assertNull(toolCall.getFunction().getArguments());
            assertEquals("function", toolCall.getType());
        } catch (IOException e) {
            fail();
        }
    }

    public void testOpenAiUnifiedStreamingProcessorParsing() throws IOException {
        // Generate random values for the JSON fields
        int toolCallIndex = randomIntBetween(0, 10);
        String toolCallId = randomAlphaOfLength(5);
        String toolCallFunctionName = randomAlphaOfLength(8);
        String toolCallFunctionArguments = randomAlphaOfLength(10);
        String toolCallType = "function";
        String toolCallJson = createToolCallJson(toolCallIndex, toolCallId, toolCallFunctionName, toolCallFunctionArguments, toolCallType);

        String choiceContent = randomAlphaOfLength(10);
        String choiceRole = randomFrom("system", "user", "assistant", "tool");
        String choiceFinishReason = randomFrom("stop", "length", "tool_calls", "content_filter", "function_call", null);
        int choiceIndex = randomIntBetween(0, 10);
        String choiceJson = createChoiceJson(choiceContent, null, choiceRole, toolCallJson, choiceFinishReason, choiceIndex);

        int usageCompletionTokens = randomIntBetween(1, 100);
        int usagePromptTokens = randomIntBetween(1, 100);
        int usageTotalTokens = randomIntBetween(1, 200);
        String usageJson = createUsageJson(usageCompletionTokens, usagePromptTokens, usageTotalTokens);

        String chatCompletionChunkId = randomAlphaOfLength(10);
        String chatCompletionChunkModel = randomAlphaOfLength(5);
        String chatCompletionChunkJson = createChatCompletionChunkJson(
            chatCompletionChunkId,
            choiceJson,
            chatCompletionChunkModel,
            "chat.completion.chunk",
            usageJson
        );

        // Parse the JSON
        XContentParserConfiguration parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(
            LoggingDeprecationHandler.INSTANCE
        );
        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, chatCompletionChunkJson)) {
            StreamingUnifiedChatCompletionResults.ChatCompletionChunk chunk = OpenAiUnifiedStreamingProcessor.ChatCompletionChunkParser
                .parse(parser);

            // Assertions to verify the parsed object
            assertEquals(chatCompletionChunkId, chunk.getId());
            assertEquals(chatCompletionChunkModel, chunk.getModel());
            assertEquals("chat.completion.chunk", chunk.getObject());
            assertNotNull(chunk.getUsage());
            assertEquals(usageCompletionTokens, chunk.getUsage().completionTokens());
            assertEquals(usagePromptTokens, chunk.getUsage().promptTokens());
            assertEquals(usageTotalTokens, chunk.getUsage().totalTokens());

            List<StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice> choices = chunk.getChoices();
            assertEquals(1, choices.size());
            StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice choice = choices.get(0);
            assertEquals(choiceContent, choice.getDelta().getContent());
            assertNull(choice.getDelta().getRefusal());
            assertEquals(choiceRole, choice.getDelta().getRole());
            assertEquals(choiceFinishReason, choice.getFinishReason());
            assertEquals(choiceIndex, choice.getIndex());

            List<StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall> toolCalls = choice.getDelta()
                .getToolCalls();
            assertEquals(1, toolCalls.size());
            StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall toolCall = toolCalls.get(0);
            assertEquals(toolCallIndex, toolCall.getIndex());
            assertEquals(toolCallId, toolCall.getId());
            assertEquals(toolCallFunctionName, toolCall.getFunction().getName());
            assertEquals(toolCallFunctionArguments, toolCall.getFunction().getArguments());
            assertEquals(toolCallType, toolCall.getType());
        }
    }

    public void testOpenAiUnifiedStreamingProcessorParsingWithNullFields() throws IOException {
        // JSON with null fields
        int choiceIndex = randomIntBetween(0, 10);
        String choiceJson = createChoiceJson(null, null, null, "", null, choiceIndex);

        String chatCompletionChunkId = randomAlphaOfLength(10);
        String chatCompletionChunkModel = randomAlphaOfLength(5);
        String chatCompletionChunkJson = createChatCompletionChunkJson(
            chatCompletionChunkId,
            choiceJson,
            chatCompletionChunkModel,
            "chat.completion.chunk",
            null
        );

        // Parse the JSON
        XContentParserConfiguration parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(
            LoggingDeprecationHandler.INSTANCE
        );
        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, chatCompletionChunkJson)) {
            StreamingUnifiedChatCompletionResults.ChatCompletionChunk chunk = OpenAiUnifiedStreamingProcessor.ChatCompletionChunkParser
                .parse(parser);

            // Assertions to verify the parsed object
            assertEquals(chatCompletionChunkId, chunk.getId());
            assertEquals(chatCompletionChunkModel, chunk.getModel());
            assertEquals("chat.completion.chunk", chunk.getObject());
            assertNull(chunk.getUsage());

            List<StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice> choices = chunk.getChoices();
            assertEquals(1, choices.size());
            StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice choice = choices.get(0);
            assertNull(choice.getDelta().getContent());
            assertNull(choice.getDelta().getRefusal());
            assertNull(choice.getDelta().getRole());
            assertNull(choice.getFinishReason());
            assertEquals(choiceIndex, choice.getIndex());
            assertTrue(choice.getDelta().getToolCalls().isEmpty());
        }
    }

    private String createToolCallJson(int index, String id, String functionName, String functionArguments, String type) {
        return Strings.format("""
            {
                "index": %d,
                "id": "%s",
                "function": {
                    "name": "%s",
                    "arguments": "%s"
                },
                "type": "%s"
            }
            """, index, id, functionName, functionArguments, type);
    }

    private String createChoiceJson(String content, String refusal, String role, String toolCallsJson, String finishReason, int index) {
        if (role == null) {
            return Strings.format(
                """
                    {
                        "delta": {
                            "content": %s,
                            "refusal": %s,
                            "tool_calls": [%s]
                        },
                        "finish_reason": %s,
                        "index": %d
                    }
                    """,
                content != null ? "\"" + content + "\"" : "null",
                refusal != null ? "\"" + refusal + "\"" : "null",
                toolCallsJson,
                finishReason != null ? "\"" + finishReason + "\"" : "null",
                index
            );
        } else {
            return Strings.format(
                """
                    {
                        "delta": {
                            "content": %s,
                            "refusal": %s,
                            "role": %s,
                            "tool_calls": [%s]
                        },
                        "finish_reason": %s,
                        "index": %d
                    }
                    """,
                content != null ? "\"" + content + "\"" : "null",
                refusal != null ? "\"" + refusal + "\"" : "null",
                role != null ? "\"" + role + "\"" : "null",
                toolCallsJson,
                finishReason != null ? "\"" + finishReason + "\"" : "null",
                index
            );
        }
    }

    private String createChatCompletionChunkJson(String id, String choicesJson, String model, String object, String usageJson) {
        if (usageJson != null) {
            return Strings.format("""
                {
                    "id": "%s",
                    "choices": [%s],
                    "model": "%s",
                    "object": "%s",
                    "usage": %s
                }
                """, id, choicesJson, model, object, usageJson);
        } else {
            return Strings.format("""
                {
                    "id": "%s",
                    "choices": [%s],
                    "model": "%s",
                    "object": "%s"
                }
                """, id, choicesJson, model, object);
        }
    }

    private String createUsageJson(int completionTokens, int promptTokens, int totalTokens) {
        return Strings.format("""
            {
                "completion_tokens": %d,
                "prompt_tokens": %d,
                "total_tokens": %d
            }
            """, completionTokens, promptTokens, totalTokens);
    }
}
