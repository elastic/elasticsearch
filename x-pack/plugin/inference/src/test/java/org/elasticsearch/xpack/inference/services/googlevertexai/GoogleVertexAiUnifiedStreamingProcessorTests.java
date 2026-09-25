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
    private static final String ASSISTANT_ROLE = "assistant";

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
            assertThat(choice.message().role(), is(ASSISTANT_ROLE));
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
            assertThat(choice.message().role(), is(ASSISTANT_ROLE));
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
            assertThat(choice.message().role(), is(ASSISTANT_ROLE));
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
            assertThat(choice.message().role(), is(ASSISTANT_ROLE));
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

        // completionTokens must include reasoning tokens so that prompt + completion == total.
        assertThat(chunk.usage().completionTokens(), is(27));
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

    /**
     * The chunk Gemini 2.5 sends when it spends the whole output budget thinking: content with a role and no parts.
     * It must end the stream with its finish reason rather than fail to parse.
     */
    public void testCandidateWithoutParts_ReportsFinishReasonAndNoContent() throws IOException {
        var chunk = parse("""
            {
              "candidates": [ {
                "content": { "role": "model" },
                "finishReason": "MAX_TOKENS",
                "index": 0
              } ],
              "usageMetadata": {
                "promptTokenCount": 10,
                "candidatesTokenCount": 0,
                "totalTokenCount": 1010,
                "thoughtsTokenCount": 1000
              },
              "modelVersion": "gemini-2.5-flash",
              "responseId": "responseId"
            }
            """);

        assertThat(chunk.choices().size(), is(1));
        var choice = chunk.choices().getFirst();
        assertThat(choice.finishReason(), is("MAX_TOKENS"));
        assertNull(choice.message().content());
        assertNull(choice.message().toolCalls());
        assertNull(choice.message().reasoning());
        assertNull(choice.message().reasoningDetails());
        assertThat(chunk.usage().completionTokens(), is(1000));
        assertThat(chunk.usage().completionTokenDetails().reasoningTokens(), is(1000));
    }

    public void testCandidateWithoutContent_ReportsFinishReason() throws IOException {
        var chunk = parse("""
            {
              "candidates": [ { "finishReason": "SAFETY", "index": 0 } ],
              "usageMetadata": { "promptTokenCount": 10, "candidatesTokenCount": 0, "totalTokenCount": 10 },
              "modelVersion": "gemini-2.5-flash",
              "responseId": "responseId"
            }
            """);

        assertThat(chunk.choices().size(), is(1));
        var choice = chunk.choices().getFirst();
        assertThat(choice.finishReason(), is("SAFETY"));
        assertNull(choice.message().content());
        // Even a candidate without content gets "assistant" on the first chunk.
        assertThat(choice.message().role(), is(ASSISTANT_ROLE));
    }

    public void testReasoningIndexKeepsCountingAcrossTheChunksOfAStream() throws IOException {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);
        var chunkParser = new GoogleVertexAiUnifiedStreamingProcessor.GoogleVertexAiChatCompletionChunkParser(false);

        var firstChunk = parseWith(chunkParser, parserConfig, thoughtChunk("First thought.", THOUGHT_SIGNATURE));
        var secondChunk = parseWith(chunkParser, parserConfig, thoughtChunk("Second thought.", OTHER_THOUGHT_SIGNATURE));

        assertThat(asTextReasoningDetail(firstChunk, 0).index(), is(0L));
        assertThat(asTextReasoningDetail(secondChunk, 0).index(), is(1L));
    }

    public void testReasoningIndexStaysSameForFragmentsOfOnThoughtBlock() throws IOException {
        // A thought block that lacks a thoughtSignature on the first chunk is still being streamed;
        // the second chunk (with the signature) completes it. Both fragments must carry the same index.
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);
        var chunkParser = new GoogleVertexAiUnifiedStreamingProcessor.GoogleVertexAiChatCompletionChunkParser(false);

        var fragmentChunk = parseWith(chunkParser, parserConfig, Strings.format("""
            {
              "candidates": [ {
                "content": {
                  "role": "model",
                  "parts": [ { "text": "Analyzing the request...", "thought": true } ]
                }
              } ],
              "usageMetadata": { "promptTokenCount": 10, "candidatesTokenCount": 5, "totalTokenCount": 15 },
              "modelVersion": "gemini-3.5-flash-lite",
              "responseId": "r1"
            }
            """));
        var terminalChunk = parseWith(chunkParser, parserConfig, thoughtChunk("...done.", THOUGHT_SIGNATURE));

        assertThat(asTextReasoningDetail(fragmentChunk, 0).index(), is(0L));
        assertThat(asTextReasoningDetail(terminalChunk, 0).index(), is(0L));
    }

    public void testFunctionCallSignatureReturnedEvenWhenExcludeReasoningIsTrue() throws IOException {
        // Thought signatures bound to function calls are opaque state that Gemini 3 needs for multi-turn
        // tool use, not user-visible reasoning content. They must be returned even when reasoning is excluded.
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
            """, FUNCTION_NAME, GOOGLE_TOOL_CALL_ID, THOUGHT_SIGNATURE), true);

        var message = chunk.choices().getFirst().message();
        assertThat(message.toolCalls().getFirst().id(), is(GOOGLE_TOOL_CALL_ID));
        // reasoning details must contain the signature even with excludeReasoning=true
        var detail = asTextReasoningDetail(chunk, 0);
        assertThat(detail.id(), is(GOOGLE_TOOL_CALL_ID));
        assertThat(detail.signature(), is(THOUGHT_SIGNATURE));
        assertNull(detail.text());
    }

    public void testReasoningIndexAdvancesWhenTextEndsAnUnsignedThoughtBlock() throws IOException {
        // Gemini typically streams: unsigned thought fragments, then answer text, then a trailing empty text part
        // carrying the thoughtSignature. The signature is a separate block and must get a different index than the
        // thought so that a client merging by index keeps them apart.
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);
        var chunkParser = new GoogleVertexAiUnifiedStreamingProcessor.GoogleVertexAiChatCompletionChunkParser(false);

        var thoughtChunk = parseWith(chunkParser, parserConfig, """
            {
              "candidates": [ {
                "content": {
                  "role": "model",
                  "parts": [ { "text": "Analyzing the request.", "thought": true } ]
                }
              } ],
              "usageMetadata": { "promptTokenCount": 10, "candidatesTokenCount": 5, "totalTokenCount": 15 },
              "modelVersion": "gemini-3.5-flash-lite",
              "responseId": "r1"
            }
            """);
        var textChunk1 = parseWith(chunkParser, parserConfig, """
            {
              "candidates": [ {
                "content": { "role": "model", "parts": [ { "text": "The answer is 42." } ] }
              } ],
              "usageMetadata": { "promptTokenCount": 10, "candidatesTokenCount": 10, "totalTokenCount": 20 },
              "modelVersion": "gemini-3.5-flash-lite",
              "responseId": "r2"
            }
            """);
        var textChunk2 = parseWith(chunkParser, parserConfig, """
            {
              "candidates": [ {
                "content": { "role": "model", "parts": [ { "text": "More details." } ] }
              } ],
              "usageMetadata": { "promptTokenCount": 10, "candidatesTokenCount": 10, "totalTokenCount": 20 },
              "modelVersion": "gemini-3.5-flash-lite",
              "responseId": "r2"
            }
            """);
        var signatureChunk = parseWith(chunkParser, parserConfig, Strings.format("""
            {
              "candidates": [ {
                "content": { "role": "model", "parts": [ { "text": "", "thoughtSignature": "%s" } ] },
                "finishReason": "STOP"
              } ],
              "usageMetadata": { "promptTokenCount": 10, "candidatesTokenCount": 20, "totalTokenCount": 30 },
              "modelVersion": "gemini-3.5-flash-lite",
              "responseId": "r3"
            }
            """, THOUGHT_SIGNATURE));

        // The thought fragment is at index 0.
        assertThat(asTextReasoningDetail(thoughtChunk, 0).index(), is(0L));
        assertThat(asTextReasoningDetail(thoughtChunk, 0).text(), is("Analyzing the request."));
        assertNull(asTextReasoningDetail(thoughtChunk, 0).signature());

        // The text chunks end the thought block; they carry no reasoning details.
        assertNull(textChunk1.choices().getFirst().message().reasoningDetails());
        assertNull(textChunk2.choices().getFirst().message().reasoningDetails());

        // The trailing signature is at index 1, not 0 — the text part ended the thought block.
        // Also confirms that consecutive content chunks don't redundantly advance the index.
        assertThat(asTextReasoningDetail(signatureChunk, 0).index(), is(1L));
        assertThat(asTextReasoningDetail(signatureChunk, 0).signature(), is(THOUGHT_SIGNATURE));
        assertNull(asTextReasoningDetail(signatureChunk, 0).text());
    }

    public void testReasoningIndexAdvancesWhenAFunctionCallEndsAnUnsignedThoughtBlock() throws IOException {
        // A function call ends the open thought block, so a second thought block starts at the next index.
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);
        var chunkParser = new GoogleVertexAiUnifiedStreamingProcessor.GoogleVertexAiChatCompletionChunkParser(false);

        var thoughtChunk = parseWith(chunkParser, parserConfig, """
            {
              "candidates": [ {
                "content": {
                  "role": "model",
                  "parts": [ { "text": "Deciding which tool to use.", "thought": true } ]
                }
              } ],
              "usageMetadata": { "promptTokenCount": 10, "candidatesTokenCount": 5, "totalTokenCount": 15 },
              "modelVersion": "gemini-3.5-flash-lite",
              "responseId": "r1"
            }
            """);
        var functionCallChunk = parseWith(chunkParser, parserConfig, Strings.format("""
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
              "responseId": "r2"
            }
            """, FUNCTION_NAME, GOOGLE_TOOL_CALL_ID, THOUGHT_SIGNATURE));
        var secondThoughtChunk = parseWith(chunkParser, parserConfig, """
            {
              "candidates": [ {
                "content": {
                  "role": "model",
                  "parts": [ { "text": "Now processing the result.", "thought": true } ]
                }
              } ],
              "usageMetadata": { "promptTokenCount": 10, "candidatesTokenCount": 5, "totalTokenCount": 15 },
              "modelVersion": "gemini-3.5-flash-lite",
              "responseId": "r3"
            }
            """);

        // First thought block at index 0.
        assertThat(asTextReasoningDetail(thoughtChunk, 0).index(), is(0L));

        // Function-call signature is id-bound; index is null.
        var fcDetail = asTextReasoningDetail(functionCallChunk, 0);
        assertThat(fcDetail.id(), is(GOOGLE_TOOL_CALL_ID));
        assertThat(fcDetail.signature(), is(THOUGHT_SIGNATURE));
        assertNull(fcDetail.index());

        // The function call ended the thought block, so the second thought is at index 1.
        assertThat(asTextReasoningDetail(secondThoughtChunk, 0).index(), is(1L));
    }

    public void testRole_IsAssistantOnTheFirstChunkOnly() throws IOException {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);
        var chunkParser = new GoogleVertexAiUnifiedStreamingProcessor.GoogleVertexAiChatCompletionChunkParser(false);

        // First chunk (thought): should get "assistant".
        var firstChunk = parseWith(chunkParser, parserConfig, thoughtChunk("Analyzing the request.", THOUGHT_SIGNATURE));
        // Second chunk (text): role consumed already, should be null.
        var secondChunk = parseWith(chunkParser, parserConfig, """
            {
              "candidates": [ {
                "content": { "role": "model", "parts": [ { "text": "The answer is 42." } ] }
              } ],
              "usageMetadata": { "promptTokenCount": 10, "candidatesTokenCount": 10, "totalTokenCount": 20 },
              "modelVersion": "gemini-3.5-flash-lite",
              "responseId": "r2"
            }
            """);
        // Third chunk (finish): role should still be null.
        var thirdChunk = parseWith(chunkParser, parserConfig, """
            {
              "candidates": [ {
                "content": { "role": "model", "parts": [ { "text": "" } ] },
                "finishReason": "STOP"
              } ],
              "usageMetadata": { "promptTokenCount": 10, "candidatesTokenCount": 12, "totalTokenCount": 22 },
              "modelVersion": "gemini-3.5-flash-lite",
              "responseId": "r3"
            }
            """);

        assertThat(firstChunk.choices().getFirst().message().role(), is(ASSISTANT_ROLE));
        assertNull(secondChunk.choices().getFirst().message().role());
        assertNull(thirdChunk.choices().getFirst().message().role());
    }

    public void testRole_UsageOnlyChunkDoesNotConsumeTheRole() throws IOException {
        // A usage-only chunk ("candidates": []) must not consume the role slot.
        // The first chunk that carries a choice should still emit "assistant".
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);
        var chunkParser = new GoogleVertexAiUnifiedStreamingProcessor.GoogleVertexAiChatCompletionChunkParser(false);

        // usage-only frame — no candidates, so candidateToChoice is never called.
        parseWith(chunkParser, parserConfig, """
            {
              "candidates": [],
              "usageMetadata": { "promptTokenCount": 10, "candidatesTokenCount": 0, "totalTokenCount": 10 },
              "modelVersion": "gemini-3.5-flash-lite",
              "responseId": "r0"
            }
            """);

        var contentChunk = parseWith(chunkParser, parserConfig, """
            {
              "candidates": [ {
                "content": { "role": "model", "parts": [ { "text": "Hello" } ] },
                "finishReason": "STOP"
              } ],
              "usageMetadata": { "promptTokenCount": 10, "candidatesTokenCount": 5, "totalTokenCount": 15 },
              "modelVersion": "gemini-3.5-flash-lite",
              "responseId": "r1"
            }
            """);

        assertThat(contentChunk.choices().getFirst().message().role(), is(ASSISTANT_ROLE));
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
