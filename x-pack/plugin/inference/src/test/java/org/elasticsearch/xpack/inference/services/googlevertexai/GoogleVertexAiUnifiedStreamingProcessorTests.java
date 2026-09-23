/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.inference.completion.ReasoningDetail.TextReasoningDetail;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.completion.ChatCompletionChunkResponse;

import java.io.IOException;
import java.util.ArrayList;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class GoogleVertexAiUnifiedStreamingProcessorTests extends ESTestCase {

    private static final String THOUGHT_SIGNATURE = "El4KXAERTTIPHPmb/yri/Qyy9cz7xqWoMPh394Dk3bIAt2jgXMJoP2cOWRyqxOs";
    private static final String OTHER_THOUGHT_SIGNATURE = "Cs8BAdHtim9zbXR0aW5nIGFub3RoZXIgc2lnbmF0dXJl";
    private static final String REASONING_FORMAT = "google-vertex-ai-v1";
    private static final String GOOGLE_TOOL_CALL_ID = "call_299965";
    private static final String FUNCTION_NAME = "schedule_meeting";

    public void testJsonLiteral() {
        String json = """
                {
                  "candidates" : [ {
                    "content" : {
                      "role" : "model",
                      "parts" : [
                        { "text" : "Elastic"  },
                        {
                          "functionCall": {
                            "name": "getWeatherData",
                            "args": { "unit": "celsius", "location": "buenos aires, argentina" }
                          }
                        }
                      ]
                    },
                    "finishReason": "MAXTOKENS"
                  } ],
                  "usageMetadata" : {
                    "promptTokenCount": 10,
                    "candidatesTokenCount": 20,
                    "totalTokenCount": 30,
                    "trafficType" : "ON_DEMAND"
                  },
                  "modelVersion" : "gemini-2.0-flash-lite",
                  "createTime" : "2025-05-07T14:36:16.122336Z",
                  "responseId" : "responseId"
                }
            """;

        XContentParserConfiguration parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(
            LoggingDeprecationHandler.INSTANCE
        );

        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, json)) {
            var chunk = GoogleVertexAiUnifiedStreamingProcessor.GoogleVertexAiChatCompletionChunkParser.parse(parser);

            assertEquals("responseId", chunk.id());
            assertEquals(1, chunk.choices().size());
            assertEquals("chat.completion.chunk", chunk.object());

            var choice = chunk.choices().getFirst();
            assertEquals("Elastic", choice.message().content());
            assertEquals("model", choice.message().role());
            assertEquals("gemini-2.0-flash-lite", chunk.model());
            assertEquals(0, choice.index()); // VertexAI response does not have Index. Use 0 as default
            assertEquals("MAXTOKENS", choice.finishReason());

            assertEquals(1, choice.message().toolCalls().size());
            var toolCall = choice.message().toolCalls().getFirst();
            assertEquals("getWeatherData", toolCall.function().name());
            assertEquals("{\"unit\":\"celsius\",\"location\":\"buenos aires, argentina\"}", toolCall.function().arguments());

            assertNotNull(chunk.usage());
            assertEquals(20, chunk.usage().completionTokens());
            assertEquals(10, chunk.usage().promptTokens());
            assertEquals(30, chunk.usage().totalTokens());

        } catch (IOException e) {
            fail("IOException during test: " + e.getMessage());
        }
    }

    public void testJsonLiteral_usageMetadataTokenCountMissing() {
        String json = """
                {
                  "candidates" : [ {
                    "content" : {
                      "role" : "model",
                      "parts" : [ { "text" : "Hello" } ]
                    },
                    "finishReason": "STOP"
                  } ],
                  "usageMetadata" : {
                    "trafficType" : "ON_DEMAND"
                  },
                  "modelVersion": "gemini-2.0-flash-001",
                  "responseId": "responseId"
                }
            """;

        XContentParserConfiguration parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(
            LoggingDeprecationHandler.INSTANCE
        );

        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, json)) {
            var chunk = GoogleVertexAiUnifiedStreamingProcessor.GoogleVertexAiChatCompletionChunkParser.parse(parser);

            assertEquals("responseId", chunk.id());
            assertEquals(1, chunk.choices().size());
            var choice = chunk.choices().getFirst();
            assertEquals("Hello", choice.message().content());
            assertEquals("model", choice.message().role());
            assertEquals("STOP", choice.finishReason());
            assertEquals(0, choice.index());
            assertNull(choice.message().toolCalls());

        } catch (IOException e) {
            fail("IOException during test: " + e.getMessage());
        }
    }

    public void testJsonLiteral_functionCallArgsMissing() {
        String json = """
                {
                  "candidates" : [ {
                    "content" : {
                      "role" : "model",
                      "parts" : [
                        {
                          "functionCall": {
                            "name": "getLocation"
                          }
                        }
                      ]
                    }
                  } ],
                  "responseId" : "resId789",
                  "modelVersion": "gemini-2.0-flash-00",
                  "usageMetadata" : {
                    "promptTokenCount": 10,
                    "candidatesTokenCount": 20,
                    "totalTokenCount": 30,
                    "trafficType" : "ON_DEMAND"
                  }
                }
            """;
        XContentParserConfiguration parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(
            LoggingDeprecationHandler.INSTANCE
        );

        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, json)) {
            var chunk = GoogleVertexAiUnifiedStreamingProcessor.GoogleVertexAiChatCompletionChunkParser.parse(parser);

            assertEquals("resId789", chunk.id());
            assertEquals(1, chunk.choices().size());
            var choice = chunk.choices().getFirst();
            assertEquals("model", choice.message().role());
            assertNull(choice.message().content());

            assertNotNull(choice.message().toolCalls());
            assertEquals(1, choice.message().toolCalls().size());
            var toolCall = choice.message().toolCalls().getFirst();
            assertEquals("getLocation", toolCall.function().name());
            assertNull(toolCall.function().arguments());

        } catch (IOException e) {
            fail("IOException during test: " + e.getMessage());
        }
    }

    public void testJsonLiteral_multipleTextParts() {
        String json = """
                {
                  "candidates" : [ {
                    "content" : {
                      "role" : "model",
                      "parts" : [
                        { "text" : "This is the first part. "  },
                        { "text" : "This is the second part." }
                      ]
                    },
                    "finishReason": "STOP"
                  } ],
                  "responseId" : "multiTextId",
                  "usageMetadata" : {
                    "promptTokenCount": 10,
                    "candidatesTokenCount": 20,
                    "totalTokenCount": 30,
                    "trafficType" : "ON_DEMAND"
                  },
                  "modelVersion": "gemini-2.0-flash-001"
                }
            """;

        XContentParserConfiguration parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(
            LoggingDeprecationHandler.INSTANCE
        );

        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, json)) {
            var chunk = GoogleVertexAiUnifiedStreamingProcessor.GoogleVertexAiChatCompletionChunkParser.parse(parser);

            assertEquals("multiTextId", chunk.id());
            assertEquals(1, chunk.choices().size());

            var choice = chunk.choices().getFirst();
            assertEquals("model", choice.message().role());
            // Verify that the text from multiple parts is concatenated
            assertEquals("This is the first part. This is the second part.", choice.message().content());
            assertEquals("STOP", choice.finishReason());
            assertEquals(0, choice.index());
            assertNull(choice.message().toolCalls());
            assertEquals("gemini-2.0-flash-001", chunk.model());
        } catch (IOException e) {
            fail("IOException during test: " + e.getMessage());
        }
    }

    public void testMultipleJsonObjectsInSingleEventAreParsed() throws IOException {
        var firstChunkData = """
            {
                "candidates": [],
                "usageMetadata": {},
                "modelVersion": "m",
                "responseId": "r1"
            }\
            """;
        var secondChunkData = """
            {
                "candidates": [],
                "usageMetadata": {},
                "modelVersion": "m",
                "responseId": "r2"
            }\
            """;
        var data = firstChunkData + "\n" + secondChunkData;
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);
        var processor = new GoogleVertexAiUnifiedStreamingProcessor(RuntimeException::new);
        var chunks = new ArrayList<>();
        processor.parse(parserConfig, data).forEachRemaining(chunks::add);
        assertThat(chunks.size(), is(2));
    }

    public void testThoughtPartBecomesReasoningAndIsKeptOutOfContent() throws IOException {
        var chunk = parse(Strings.format("""
            {
              "candidates": [ {
                "content": {
                  "role": "model",
                  "parts": [
                    { "text": "Working out the timezone.", "thought": true, "thoughtSignature": "%s" },
                    { "text": "The meeting is at 10:00." }
                  ]
                },
                "finishReason": "STOP"
              } ],
              "usageMetadata": { "promptTokenCount": 10, "candidatesTokenCount": 20, "totalTokenCount": 30 },
              "modelVersion": "gemini-3.5-flash-lite",
              "responseId": "responseId"
            }
            """, THOUGHT_SIGNATURE));

        var message = chunk.choices().getFirst().message();
        assertThat(message.content(), is("The meeting is at 10:00."));
        assertThat(message.reasoning(), is("Working out the timezone."));

        assertThat(message.reasoningDetails().size(), is(1));
        var detail = asTextReasoningDetail(chunk, 0);
        assertThat(detail.format(), is(REASONING_FORMAT));
        assertThat(detail.index(), is(0L));
        assertThat(detail.text(), is("Working out the timezone."));
        assertThat(detail.signature(), is(THOUGHT_SIGNATURE));
        assertNull(detail.id());
    }

    public void testFunctionCallSignatureIsBoundToTheToolCallId() throws IOException {
        var chunk = parse(Strings.format("""
            {
              "candidates": [ {
                "content": {
                  "role": "model",
                  "parts": [
                    {
                      "functionCall": { "name": "%s", "args": { "topic": "Q3 planning" }, "id": "%s" },
                      "thoughtSignature": "%s"
                    }
                  ]
                },
                "finishReason": "STOP"
              } ],
              "usageMetadata": { "promptTokenCount": 10, "candidatesTokenCount": 20, "totalTokenCount": 30 },
              "modelVersion": "gemini-3.5-flash-lite",
              "responseId": "responseId"
            }
            """, FUNCTION_NAME, GOOGLE_TOOL_CALL_ID, THOUGHT_SIGNATURE));

        var message = chunk.choices().getFirst().message();
        assertThat(message.toolCalls().getFirst().id(), is(GOOGLE_TOOL_CALL_ID));

        var detail = asTextReasoningDetail(chunk, 0);
        assertThat(detail.id(), is(GOOGLE_TOOL_CALL_ID));
        assertThat(detail.signature(), is(THOUGHT_SIGNATURE));
        assertNull(detail.text());
        assertNull(detail.index());
    }

    public void testFunctionCallIdFallsBackToTheNameWhenAbsent() throws IOException {
        var chunk = parse(Strings.format("""
            {
              "candidates": [ {
                "content": {
                  "role": "model",
                  "parts": [
                    {
                      "functionCall": { "name": "%s", "args": { "topic": "Q3 planning" } },
                      "thoughtSignature": "%s"
                    }
                  ]
                }
              } ],
              "usageMetadata": { "promptTokenCount": 10, "candidatesTokenCount": 20, "totalTokenCount": 30 },
              "modelVersion": "gemini-2.0-flash-lite",
              "responseId": "responseId"
            }
            """, FUNCTION_NAME, THOUGHT_SIGNATURE));

        var message = chunk.choices().getFirst().message();
        assertThat(message.toolCalls().getFirst().id(), is(FUNCTION_NAME));
        assertThat(asTextReasoningDetail(chunk, 0).id(), is(FUNCTION_NAME));
    }

    public void testSignatureOnAPlainTextPartIsIndexed() throws IOException {
        var chunk = parse(Strings.format("""
            {
              "candidates": [ {
                "content": {
                  "role": "model",
                  "parts": [ { "text": "The meeting is at 10:00.", "thoughtSignature": "%s" } ]
                }
              } ],
              "usageMetadata": { "promptTokenCount": 10, "candidatesTokenCount": 20, "totalTokenCount": 30 },
              "modelVersion": "gemini-3.5-flash-lite",
              "responseId": "responseId"
            }
            """, THOUGHT_SIGNATURE));

        var message = chunk.choices().getFirst().message();
        assertThat(message.content(), is("The meeting is at 10:00."));
        assertNull(message.reasoning());

        var detail = asTextReasoningDetail(chunk, 0);
        assertThat(detail.index(), is(0L));
        assertThat(detail.signature(), is(THOUGHT_SIGNATURE));
        assertNull(detail.text());
        assertNull(detail.id());
    }

    public void testExcludeReasoningDropsReasoningButStillKeepsThoughtTextOutOfContent() throws IOException {
        var chunk = parse(Strings.format("""
            {
              "candidates": [ {
                "content": {
                  "role": "model",
                  "parts": [
                    { "text": "Working out the timezone.", "thought": true, "thoughtSignature": "%s" },
                    { "text": "The meeting is at 10:00." }
                  ]
                }
              } ],
              "usageMetadata": { "promptTokenCount": 10, "candidatesTokenCount": 20, "totalTokenCount": 30 },
              "modelVersion": "gemini-3.5-flash-lite",
              "responseId": "responseId"
            }
            """, THOUGHT_SIGNATURE), true);

        var message = chunk.choices().getFirst().message();
        assertThat(message.content(), is("The meeting is at 10:00."));
        assertNull(message.reasoning());
        assertNull(message.reasoningDetails());
    }

    public void testThoughtsTokenCountSurfacesAsReasoningTokens() throws IOException {
        var chunk = parse("""
            {
              "candidates": [ {
                "content": { "role": "model", "parts": [ { "text": "Hello" } ] }
              } ],
              "usageMetadata": {
                "promptTokenCount": 10,
                "candidatesTokenCount": 20,
                "totalTokenCount": 30,
                "thoughtsTokenCount": 7
              },
              "modelVersion": "gemini-3.5-flash-lite",
              "responseId": "responseId"
            }
            """);

        assertThat(chunk.usage().completionTokenDetails().reasoningTokens(), is(7));
    }

    public void testUsageWithoutThoughtsTokenCountHasNoCompletionTokenDetails() throws IOException {
        var chunk = parse("""
            {
              "candidates": [ {
                "content": { "role": "model", "parts": [ { "text": "Hello" } ] }
              } ],
              "usageMetadata": { "promptTokenCount": 10, "candidatesTokenCount": 20, "totalTokenCount": 30 },
              "modelVersion": "gemini-2.0-flash-lite",
              "responseId": "responseId"
            }
            """);

        assertThat(chunk.usage().completionTokenDetails(), is(nullValue()));
    }

    public void testReasoningIndexKeepsCountingAcrossTheChunksOfAStream() throws IOException {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);
        var chunkParser = new GoogleVertexAiUnifiedStreamingProcessor.GoogleVertexAiChatCompletionChunkParser(false);

        var firstChunk = parseWith(chunkParser, parserConfig, thoughtChunk("First thought.", THOUGHT_SIGNATURE));
        var secondChunk = parseWith(chunkParser, parserConfig, thoughtChunk("Second thought.", OTHER_THOUGHT_SIGNATURE));

        assertThat(asTextReasoningDetail(firstChunk, 0).index(), is(0L));
        assertThat(asTextReasoningDetail(secondChunk, 0).index(), is(1L));
    }

    private static String thoughtChunk(String thought, String signature) {
        return Strings.format("""
            {
              "candidates": [ {
                "content": {
                  "role": "model",
                  "parts": [ { "text": "%s", "thought": true, "thoughtSignature": "%s" } ]
                }
              } ],
              "usageMetadata": { "promptTokenCount": 10, "candidatesTokenCount": 20, "totalTokenCount": 30 },
              "modelVersion": "gemini-3.5-flash-lite",
              "responseId": "responseId"
            }
            """, thought, signature);
    }

    private static TextReasoningDetail asTextReasoningDetail(ChatCompletionChunkResponse chunk, int index) {
        return (TextReasoningDetail) chunk.choices().getFirst().message().reasoningDetails().get(index);
    }

    private static ChatCompletionChunkResponse parse(String json) throws IOException {
        return parse(json, false);
    }

    private static ChatCompletionChunkResponse parse(String json, boolean excludeReasoning) throws IOException {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);
        var chunkParser = new GoogleVertexAiUnifiedStreamingProcessor.GoogleVertexAiChatCompletionChunkParser(excludeReasoning);
        return parseWith(chunkParser, parserConfig, json);
    }

    private static ChatCompletionChunkResponse parseWith(
        GoogleVertexAiUnifiedStreamingProcessor.GoogleVertexAiChatCompletionChunkParser chunkParser,
        XContentParserConfiguration parserConfig,
        String json
    ) throws IOException {
        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, json)) {
            return chunkParser.parseChunk(parser);
        }
    }
}
