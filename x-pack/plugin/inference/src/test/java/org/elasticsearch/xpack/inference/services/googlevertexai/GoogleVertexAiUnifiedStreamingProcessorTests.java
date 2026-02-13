/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai;

import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

public class GoogleVertexAiUnifiedStreamingProcessorTests extends ESTestCase {

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
            assertEquals("Elastic", choice.delta().content());
            assertEquals("model", choice.delta().role());
            assertEquals("gemini-2.0-flash-lite", chunk.model());
            assertEquals(0, choice.index()); // VertexAI response does not have Index. Use 0 as default
            assertEquals("MAXTOKENS", choice.finishReason());

            assertEquals(1, choice.delta().toolCalls().size());
            var toolCall = choice.delta().toolCalls().getFirst();
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
            assertEquals("Hello", choice.delta().content());
            assertEquals("model", choice.delta().role());
            assertEquals("STOP", choice.finishReason());
            assertEquals(0, choice.index());
            assertNull(choice.delta().toolCalls());

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
            assertEquals("model", choice.delta().role());
            assertNull(choice.delta().content());

            assertNotNull(choice.delta().toolCalls());
            assertEquals(1, choice.delta().toolCalls().size());
            var toolCall = choice.delta().toolCalls().getFirst();
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
            assertEquals("model", choice.delta().role());
            // Verify that the text from multiple parts is concatenated
            assertEquals("This is the first part. This is the second part.", choice.delta().content());
            assertEquals("STOP", choice.finishReason());
            assertEquals(0, choice.index());
            assertNull(choice.delta().toolCalls());
            assertEquals("gemini-2.0-flash-001", chunk.model());
        } catch (IOException e) {
            fail("IOException during test: " + e.getMessage());
        }
    }
}
