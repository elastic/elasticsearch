/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.completion.ReasoningDetail;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.StreamingUnifiedChatCompletionResults;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEvent;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;

import static org.elasticsearch.xpack.inference.common.DelegatingProcessorTests.onNext;
import static org.hamcrest.Matchers.is;

public class OpenAiUnifiedStreamingProcessorTests extends ESTestCase {

    private static final String NULL_JSON_VALUE = "null";

    /**
     * A usage chunk as emitted by an OpenAI-compatible provider that sends explicit JSON {@code null}s for both token details
     * fields. Reproduced verbatim from a reported parse failure, so prefer not to parameterize the token counts here.
     */
    private static final String CHUNK_WITH_NULL_TOKEN_DETAILS_JSON = """
        {
          "id": "example_id",
          "choices": [],
          "model": "example_model",
          "object": "chat.completion.chunk",
          "usage": {
            "prompt_tokens": 53,
            "completion_tokens": 50,
            "total_tokens": 103,
            "prompt_tokens_details": null,
            "completion_tokens_details": null
          }
        }
        """;

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
            assertEquals("example_id", chunk.id());
            assertEquals("example_model", chunk.model());
            assertEquals("chat.completion.chunk", chunk.object());
            assertNotNull(chunk.usage());
            assertEquals(50, chunk.usage().completionTokens());
            assertEquals(20, chunk.usage().promptTokens());
            assertEquals(70, chunk.usage().totalTokens());

            List<StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice> choices = chunk.choices();
            assertEquals(1, choices.size());
            StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice choice = choices.get(0);
            assertEquals("example_content", choice.delta().content());
            assertNull(choice.delta().refusal());
            assertEquals("assistant", choice.delta().role());
            assertEquals("stop", choice.finishReason());
            assertEquals(0, choice.index());

            List<StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall> toolCalls = choice.delta().toolCalls();
            assertEquals(1, toolCalls.size());
            StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall toolCall = toolCalls.get(0);
            assertEquals(1, toolCall.index());
            assertEquals("tool_call_id", toolCall.id());
            assertEquals("example_function_name", toolCall.function().name());
            assertEquals("example_arguments", toolCall.function().arguments());
            assertEquals("function", toolCall.type());
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
                  "usage": null
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
            assertEquals("example_id", chunk.id());
            assertEquals("example_model", chunk.model());
            assertEquals("chat.completion.chunk", chunk.object());
            assertNull(chunk.usage());

            List<StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice> choices = chunk.choices();
            assertEquals(2, choices.size());

            // First choice assertions
            StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice firstChoice = choices.get(0);
            assertNull(firstChoice.delta().content());
            assertNull(firstChoice.delta().refusal());
            assertEquals("assistant", firstChoice.delta().role());
            assertTrue(firstChoice.delta().toolCalls().isEmpty());
            assertNull(firstChoice.finishReason());
            assertEquals(0, firstChoice.index());

            // Second choice assertions
            StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice secondChoice = choices.get(1);
            assertEquals("example_content", secondChoice.delta().content());
            assertEquals("example_refusal", secondChoice.delta().refusal());
            assertEquals("user", secondChoice.delta().role());
            assertEquals("stop", secondChoice.finishReason());
            assertEquals(1, secondChoice.index());

            List<StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall> toolCalls = secondChoice.delta()
                .toolCalls();
            assertEquals(1, toolCalls.size());
            StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall toolCall = toolCalls.get(0);
            assertEquals(1, toolCall.index());
            assertNull(toolCall.id());
            assertEquals("example_function_name", toolCall.function().name());
            assertNull(toolCall.function().arguments());
            assertEquals("function", toolCall.type());
        } catch (IOException e) {
            fail();
        }
    }

    public void testJsonNullFunctionName() throws IOException {
        String json = """
            {
                "object": "chat.completion.chunk",
                "id": "",
                "created": 1746800254,
                "model": "/repository",
                "system_fingerprint": "3.2.3-sha-a1f3ebe",
                "choices": [
                    {
                        "index": 0,
                        "delta": {
                            "role": "assistant",
                            "tool_calls": [
                                {
                                    "index": 0,
                                    "id": "8f7c27be-6803-48e6-bba4-8cdcbcd2ff9a",
                                    "type": "function",
                                    "function": {
                                        "name": null,
                                        "arguments": " \\\""
                                    }
                                }
                            ]
                        },
                        "logprobs": null,
                        "finish_reason": null
                    }
                ],
                "usage": null
            }
            """;

        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(XContentParserConfiguration.EMPTY, json)) {
            StreamingUnifiedChatCompletionResults.ChatCompletionChunk chunk = OpenAiUnifiedStreamingProcessor.ChatCompletionChunkParser
                .parse(parser);

            // Assertions to verify the parsed object
            assertThat(chunk.id(), is(""));
            assertThat(chunk.model(), is("/repository"));
            assertThat(chunk.object(), is("chat.completion.chunk"));
            assertNull(chunk.usage());

            List<StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice> choices = chunk.choices();
            assertThat(choices.size(), is(1));

            // First choice assertions
            StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice firstChoice = choices.get(0);
            assertNull(firstChoice.delta().content());
            assertNull(firstChoice.delta().refusal());
            assertThat(firstChoice.delta().role(), is("assistant"));
            assertNull(firstChoice.finishReason());
            assertThat(firstChoice.index(), is(0));

            List<StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall> toolCalls = firstChoice.delta()
                .toolCalls();
            assertThat(toolCalls.size(), is(1));

            StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall toolCall = toolCalls.get(0);
            assertThat(toolCall.index(), is(0));
            assertThat(toolCall.id(), is("8f7c27be-6803-48e6-bba4-8cdcbcd2ff9a"));
            assertThat(toolCall.type(), is("function"));
            assertNull(toolCall.function().name());
            assertThat(toolCall.function().arguments(), is(" \""));
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
        int reasoningTokens = randomIntBetween(1, 50);
        String usageJson = createUsageJson(usageCompletionTokens, usagePromptTokens, usageTotalTokens, null, reasoningTokens);

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
            assertEquals(chatCompletionChunkId, chunk.id());
            assertEquals(chatCompletionChunkModel, chunk.model());
            assertEquals("chat.completion.chunk", chunk.object());
            assertNotNull(chunk.usage());
            assertEquals(usageCompletionTokens, chunk.usage().completionTokens());
            assertEquals(usagePromptTokens, chunk.usage().promptTokens());
            assertEquals(usageTotalTokens, chunk.usage().totalTokens());
            assertNotNull(chunk.usage().completionTokenDetails());
            assertNotNull(chunk.usage().completionTokenDetails().reasoningTokens());
            assertEquals(reasoningTokens, (int) chunk.usage().completionTokenDetails().reasoningTokens());

            List<StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice> choices = chunk.choices();
            assertEquals(1, choices.size());
            StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice choice = choices.get(0);
            assertEquals(choiceContent, choice.delta().content());
            assertNull(choice.delta().refusal());
            assertEquals(choiceRole, choice.delta().role());
            assertEquals("some_reasoning", choice.delta().reasoning());
            assertEquals(
                List.of(
                    new ReasoningDetail.EncryptedReasoningDetail(
                        "some_encrypted_reasoning_detail_format",
                        "some_id_0",
                        0L,
                        "some_encrypted_data"
                    ),
                    new ReasoningDetail.SummaryReasoningDetail("some_summary_reasoning_detail_format", "some_id_1", 1L, "some_summary"),
                    new ReasoningDetail.TextReasoningDetail(
                        "some_text_reasoning_detail_format",
                        "some_id_2",
                        2L,
                        "some_text",
                        "some_signature"
                    )
                ),
                choice.delta().reasoningDetails()
            );
            assertEquals(choiceFinishReason, choice.finishReason());
            assertEquals(choiceIndex, choice.index());

            List<StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall> toolCalls = choice.delta().toolCalls();
            assertEquals(1, toolCalls.size());
            StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall toolCall = toolCalls.get(0);
            assertEquals(toolCallIndex, toolCall.index());
            assertEquals(toolCallId, toolCall.id());
            assertEquals(toolCallFunctionName, toolCall.function().name());
            assertEquals(toolCallFunctionArguments, toolCall.function().arguments());
            assertEquals(toolCallType, toolCall.type());
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
            assertEquals(chatCompletionChunkId, chunk.id());
            assertEquals(chatCompletionChunkModel, chunk.model());
            assertEquals("chat.completion.chunk", chunk.object());
            assertNull(chunk.usage());

            List<StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice> choices = chunk.choices();
            assertEquals(1, choices.size());
            StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice choice = choices.get(0);
            assertNull(choice.delta().content());
            assertNull(choice.delta().refusal());
            assertNull(choice.delta().role());
            assertNull(choice.finishReason());
            assertEquals(choiceIndex, choice.index());
            assertTrue(choice.delta().toolCalls().isEmpty());
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
                            "tool_calls": [%s],
                            "reasoning": "some_reasoning",
                            "reasoning_details": [
                                {
                                    "type": "reasoning.encrypted",
                                    "format": "some_encrypted_reasoning_detail_format",
                                    "id": "some_id_0",
                                    "index": 0,
                                    "data": "some_encrypted_data"
                                },
                                {
                                    "type": "reasoning.summary",
                                    "format": "some_summary_reasoning_detail_format",
                                    "id": "some_id_1",
                                    "index": 1,
                                    "summary": "some_summary"
                                },
                                {
                                    "type": "reasoning.text",
                                    "format": "some_text_reasoning_detail_format",
                                    "id": "some_id_2",
                                    "index": 2,
                                    "text": "some_text",
                                    "signature": "some_signature"
                                }
                            ]
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

    private String createUsageJson(
        int completionTokens,
        int promptTokens,
        int totalTokens,
        @Nullable Integer cachedTokens,
        @Nullable Integer reasoningTokens
    ) {
        return createUsageJsonWithRawTokenDetails(
            completionTokens,
            promptTokens,
            totalTokens,
            cachedTokens != null ? createPromptTokensDetailsJson(String.valueOf(cachedTokens)) : null,
            reasoningTokens != null ? createCompletionTokensDetailsJson(String.valueOf(reasoningTokens)) : null
        );
    }

    /** Takes the count as a raw JSON fragment so tests can pass {@link #NULL_JSON_VALUE}. */
    private String createPromptTokensDetailsJson(String cachedTokensJson) {
        return Strings.format("""
            {
                "cached_tokens": %s
            }""", cachedTokensJson);
    }

    private String createCompletionTokensDetailsJson(String reasoningTokensJson) {
        return Strings.format("""
            {
                "reasoning_tokens": %s
            }""", reasoningTokensJson);
    }

    /**
     * Creates a {@code usage} object with both token details fields inlined verbatim, so tests can distinguish a field that is
     * absent from one that is explicitly {@code null}. A {@code null} fragment omits the field entirely; any other value -
     * including {@link #NULL_JSON_VALUE} - is written as-is. This is the same convention
     * {@link #createChatCompletionChunkJson} already uses for its {@code usageJson} argument.
     */
    private String createUsageJsonWithRawTokenDetails(
        int completionTokens,
        int promptTokens,
        int totalTokens,
        @Nullable String promptTokensDetailsJson,
        @Nullable String completionTokensDetailsJson
    ) {
        var promptTokensDetailsPart = promptTokensDetailsJson != null ? Strings.format("""
            ,
            "prompt_tokens_details": %s""", promptTokensDetailsJson) : "";
        var completionTokensDetailsPart = completionTokensDetailsJson != null ? Strings.format("""
            ,
            "completion_tokens_details": %s""", completionTokensDetailsJson) : "";
        return Strings.format("""
            {
                "completion_tokens": %d,
                "prompt_tokens": %d,
                "total_tokens": %d\
                %s\
                %s
            }
            """, completionTokens, promptTokens, totalTokens, promptTokensDetailsPart, completionTokensDetailsPart);
    }

    private StreamingUnifiedChatCompletionResults.ChatCompletionChunk parseChunk(String chunkJson) throws IOException {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);
        try (var parser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, chunkJson)) {
            return OpenAiUnifiedStreamingProcessor.ChatCompletionChunkParser.parse(parser);
        }
    }

    private StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Usage parseUsage(String usageJson) throws IOException {
        var chunk = parseChunk(
            createChatCompletionChunkJson(
                randomAlphaOfLength(10),
                createChoiceJson(null, null, null, "", null, 0),
                randomAlphaOfLength(5),
                "chat.completion.chunk",
                usageJson
            )
        );

        assertNotNull(chunk.usage());
        return chunk.usage();
    }

    public void testUsageParsingWithCachedAndReasoningTokens() throws IOException {
        testUsageParsing(true, true);
    }

    public void testUsageParsingWithoutCachedAndReasoningTokens() throws IOException {
        testUsageParsing(false, false);
    }

    public void testUsageParsingWithReasoningTokens() throws IOException {
        testUsageParsing(false, true);
    }

    public void testUsageParsingWithCachedTokens() throws IOException {
        testUsageParsing(true, false);
    }

    private void testUsageParsing(boolean includeCachedTokens, boolean includeReasoningTokens) throws IOException {
        int completionTokens = randomIntBetween(1, 100);
        int promptTokens = randomIntBetween(1, 100);
        int totalTokens = randomIntBetween(1, 200);
        var cachedTokens = includeCachedTokens ? randomIntBetween(1, 50) : null;
        var reasoningTokens = includeReasoningTokens ? randomIntBetween(1, 50) : null;

        String usageJson = createUsageJson(completionTokens, promptTokens, totalTokens, cachedTokens, reasoningTokens);

        String chatCompletionChunkJson = createChatCompletionChunkJson(
            randomAlphaOfLength(10),
            createChoiceJson(null, null, null, "", null, 0),
            randomAlphaOfLength(5),
            "chat.completion.chunk",
            usageJson
        );

        XContentParserConfiguration parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(
            LoggingDeprecationHandler.INSTANCE
        );
        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, chatCompletionChunkJson)) {
            StreamingUnifiedChatCompletionResults.ChatCompletionChunk chunk = OpenAiUnifiedStreamingProcessor.ChatCompletionChunkParser
                .parse(parser);

            assertNotNull(chunk.usage());
            assertEquals(completionTokens, chunk.usage().completionTokens());
            assertEquals(promptTokens, chunk.usage().promptTokens());
            assertEquals(totalTokens, chunk.usage().totalTokens());
            assertEquals(cachedTokens, chunk.usage().cachedTokens());
            if (includeReasoningTokens) {
                assertNotNull(chunk.usage().completionTokenDetails());
                assertEquals(reasoningTokens, chunk.usage().completionTokenDetails().reasoningTokens());
            } else {
                assertNull(chunk.usage().completionTokenDetails());
            }
        }
    }

    public void testJsonLiteral_NullTokenDetails() throws IOException {
        var chunk = parseChunk(CHUNK_WITH_NULL_TOKEN_DETAILS_JSON);

        assertThat(chunk.id(), is("example_id"));
        assertThat(chunk.model(), is("example_model"));
        assertThat(chunk.object(), is("chat.completion.chunk"));
        assertTrue(chunk.choices().isEmpty());
        assertNotNull(chunk.usage());
        assertThat(chunk.usage().completionTokens(), is(50));
        assertThat(chunk.usage().promptTokens(), is(53));
        assertThat(chunk.usage().totalTokens(), is(103));
        assertNull(chunk.usage().cachedTokens());
        assertNull(chunk.usage().completionTokenDetails());
    }

    public void testUsageParsing_NullPromptAndCompletionTokenDetails() throws IOException {
        var completionTokens = randomIntBetween(1, 100);
        var promptTokens = randomIntBetween(1, 100);
        var totalTokens = randomIntBetween(1, 200);

        var usage = parseUsage(
            createUsageJsonWithRawTokenDetails(completionTokens, promptTokens, totalTokens, NULL_JSON_VALUE, NULL_JSON_VALUE)
        );

        assertThat(usage.completionTokens(), is(completionTokens));
        assertThat(usage.promptTokens(), is(promptTokens));
        assertThat(usage.totalTokens(), is(totalTokens));
        assertNull(usage.cachedTokens());
        assertNull(usage.completionTokenDetails());
    }

    public void testUsageParsing_NullPromptTokensDetails() throws IOException {
        var completionTokens = randomIntBetween(1, 100);
        var promptTokens = randomIntBetween(1, 100);
        var totalTokens = randomIntBetween(1, 200);
        var reasoningTokens = randomIntBetween(1, 50);

        var usage = parseUsage(
            createUsageJsonWithRawTokenDetails(
                completionTokens,
                promptTokens,
                totalTokens,
                NULL_JSON_VALUE,
                createCompletionTokensDetailsJson(String.valueOf(reasoningTokens))
            )
        );

        assertThat(usage.completionTokens(), is(completionTokens));
        assertThat(usage.promptTokens(), is(promptTokens));
        assertThat(usage.totalTokens(), is(totalTokens));
        assertNull(usage.cachedTokens());
        // an explicit null for one details field must not swallow its sibling
        assertNotNull(usage.completionTokenDetails());
        assertThat(usage.completionTokenDetails().reasoningTokens(), is(reasoningTokens));
    }

    public void testUsageParsing_NullCompletionTokensDetails() throws IOException {
        var completionTokens = randomIntBetween(1, 100);
        var promptTokens = randomIntBetween(1, 100);
        var totalTokens = randomIntBetween(1, 200);
        var cachedTokens = randomIntBetween(1, 50);

        var usage = parseUsage(
            createUsageJsonWithRawTokenDetails(
                completionTokens,
                promptTokens,
                totalTokens,
                createPromptTokensDetailsJson(String.valueOf(cachedTokens)),
                NULL_JSON_VALUE
            )
        );

        assertThat(usage.completionTokens(), is(completionTokens));
        assertThat(usage.promptTokens(), is(promptTokens));
        assertThat(usage.totalTokens(), is(totalTokens));
        assertThat(usage.cachedTokens(), is(cachedTokens));
        assertNull(usage.completionTokenDetails());
    }

    public void testUsageParsing_NullCachedTokens() throws IOException {
        var completionTokens = randomIntBetween(1, 100);
        var promptTokens = randomIntBetween(1, 100);
        var totalTokens = randomIntBetween(1, 200);

        var usage = parseUsage(
            createUsageJsonWithRawTokenDetails(
                completionTokens,
                promptTokens,
                totalTokens,
                createPromptTokensDetailsJson(NULL_JSON_VALUE),
                null
            )
        );

        assertThat(usage.completionTokens(), is(completionTokens));
        assertThat(usage.promptTokens(), is(promptTokens));
        assertThat(usage.totalTokens(), is(totalTokens));
        // PromptTokensDetailsParser returns Integer directly; null cached_tokens causes the parser to return null itself
        assertNull(usage.cachedTokens());
        assertNull(usage.completionTokenDetails());
    }

    public void testUsageParsing_NullReasoningTokens() throws IOException {
        var completionTokens = randomIntBetween(1, 100);
        var promptTokens = randomIntBetween(1, 100);
        var totalTokens = randomIntBetween(1, 200);

        var usage = parseUsage(
            createUsageJsonWithRawTokenDetails(
                completionTokens,
                promptTokens,
                totalTokens,
                null,
                createCompletionTokensDetailsJson(NULL_JSON_VALUE)
            )
        );

        assertThat(usage.completionTokens(), is(completionTokens));
        assertThat(usage.promptTokens(), is(promptTokens));
        assertThat(usage.totalTokens(), is(totalTokens));
        assertNull(usage.cachedTokens());
        // CompletionTokensDetailsParser wraps in new CompletionTokenDetails(...), so the record itself is non-null
        // even when reasoning_tokens is null — unlike PromptTokensDetailsParser which returns Integer directly.
        assertNotNull(usage.completionTokenDetails());
        assertNull(usage.completionTokenDetails().reasoningTokens());
    }

    public void testUsageParsing_NullCachedAndReasoningTokens() throws IOException {
        var completionTokens = randomIntBetween(1, 100);
        var promptTokens = randomIntBetween(1, 100);
        var totalTokens = randomIntBetween(1, 200);

        var usage = parseUsage(
            createUsageJsonWithRawTokenDetails(
                completionTokens,
                promptTokens,
                totalTokens,
                createPromptTokensDetailsJson(NULL_JSON_VALUE),
                createCompletionTokensDetailsJson(NULL_JSON_VALUE)
            )
        );

        assertThat(usage.completionTokens(), is(completionTokens));
        assertThat(usage.promptTokens(), is(promptTokens));
        assertThat(usage.totalTokens(), is(totalTokens));
        assertNull(usage.cachedTokens());
        assertNotNull(usage.completionTokenDetails());
        assertNull(usage.completionTokenDetails().reasoningTokens());
    }

    public void testUsageParsing_NullTokenDetailsDoesNotFailTheStream() {
        var events = new ArrayDeque<ServerSentEvent>();
        events.offer(new ServerSentEvent(CHUNK_WITH_NULL_TOKEN_DETAILS_JSON));

        // onNext asserts that the processor never calls downstream.onError
        var results = onNext(new OpenAiUnifiedStreamingProcessor(IllegalStateException::new), events);

        assertThat(results.chunks().size(), is(1));
        var usage = results.chunks().getFirst().usage();
        assertNotNull(usage);
        assertNull(usage.cachedTokens());
        assertNull(usage.completionTokenDetails());
    }

    public void testMultipleJsonObjectsInSingleEventAreParsed() throws IOException {
        var firstChunkData = """
            {
                "id": "1",
                "choices": [],
                "model": "m",
                "object": "chat.completion.chunk"
            }\
            """;
        var secondChunkData = """
            {
                "id": "2",
                "choices": [],
                "model": "m",
                "object": "chat.completion.chunk"
            }\
            """;
        var data = firstChunkData + "\n" + secondChunkData;
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);
        var chunks = OpenAiUnifiedStreamingProcessor.parse(parserConfig, data).toList();
        assertThat(chunks.size(), is(2));
        assertThat(chunks.get(0).id(), is("1"));
        assertThat(chunks.get(1).id(), is("2"));
    }
}
