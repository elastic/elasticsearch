/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.request;

import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.Utils.assertJsonEquals;
import static org.hamcrest.Matchers.containsString;

public class GoogleVertexAiUnifiedChatCompletionRequestEntityTests extends ESTestCase {

    private static final String USER_ROLE = "user";
    private static final String ASSISTANT_ROLE = "assistant";

    public void testBasicSerialization_SingleMessage() throws IOException {
        UnifiedCompletionRequest.Message message = new UnifiedCompletionRequest.Message(
            new UnifiedCompletionRequest.ContentString("Hello, Vertex AI!"),
            USER_ROLE,
            null,
            null
        );
        var messageList = new ArrayList<UnifiedCompletionRequest.Message>();
        messageList.add(message);

        var unifiedRequest = UnifiedCompletionRequest.of(messageList);
        UnifiedChatInput unifiedChatInput = new UnifiedChatInput(unifiedRequest, true); // stream doesn't affect VertexAI request body
        GoogleVertexAiUnifiedChatCompletionRequestEntity entity = new GoogleVertexAiUnifiedChatCompletionRequestEntity(unifiedChatInput);

        XContentBuilder builder = JsonXContent.contentBuilder();
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String jsonString = Strings.toString(builder);
        String expectedJson = """
            {
                "contents": [
                    {
                        "role": "user",
                        "parts": [
                            {
                                "text": "Hello, Vertex AI!"
                            }
                        ]
                    }
                ]
            }
            """;
        assertJsonEquals(jsonString, expectedJson);
    }

    public void testSerialization_MultipleMessages() throws IOException {
        var messages = List.of(
            new UnifiedCompletionRequest.Message(
                new UnifiedCompletionRequest.ContentString("Previous user message."),
                USER_ROLE,
                null,
                null
            ),
            new UnifiedCompletionRequest.Message(
                new UnifiedCompletionRequest.ContentString("Previous model response."),
                ASSISTANT_ROLE,
                null,
                null
            ),
            new UnifiedCompletionRequest.Message(new UnifiedCompletionRequest.ContentString("Current user query."), USER_ROLE, null, null)
        );

        var unifiedRequest = UnifiedCompletionRequest.of(messages);
        UnifiedChatInput unifiedChatInput = new UnifiedChatInput(unifiedRequest, false);

        GoogleVertexAiUnifiedChatCompletionRequestEntity entity = new GoogleVertexAiUnifiedChatCompletionRequestEntity(unifiedChatInput);

        XContentBuilder builder = JsonXContent.contentBuilder();
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String jsonString = Strings.toString(builder);
        String expectedJson = """
            {
                "contents": [
                    {
                        "role": "user",
                        "parts": [ { "text": "Previous user message." } ]
                    },
                    {
                        "role": "model",
                        "parts": [ { "text": "Previous model response." } ]
                    },
                    {
                        "role": "user",
                        "parts": [ { "text": "Current user query." } ]
                    }
                ]
            }
            """;
        assertJsonEquals(jsonString, expectedJson);
    }

    public void testSerialization_Tools() throws IOException {
        var request = new UnifiedCompletionRequest(
            List.of(
                new UnifiedCompletionRequest.Message(
                    new UnifiedCompletionRequest.ContentObjects(List.of(new UnifiedCompletionRequest.ContentObject("some text", "text"))),
                    "user",
                    null,
                    null
                )
            ),
            "gemini-2.0",
            null,
            null,
            null,
            null,
            List.of(
                new UnifiedCompletionRequest.Tool(
                    "function",
                    new UnifiedCompletionRequest.Tool.FunctionField(
                        "Get the current weather in a given location",
                        "get_current_weather",
                        Map.of("type", "object", "description", "a description"),
                        null
                    )
                )
            ),
            null
        );
        UnifiedChatInput unifiedChatInput = new UnifiedChatInput(request, false);

        GoogleVertexAiUnifiedChatCompletionRequestEntity entity = new GoogleVertexAiUnifiedChatCompletionRequestEntity(unifiedChatInput);

        XContentBuilder builder = JsonXContent.contentBuilder();
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String jsonString = Strings.toString(builder);
        String expectedJson = """
            {
                "contents": [
                    {
                        "role": "user",
                        "parts": [ { "text": "some text" } ]
                    }
                ],
                "tools": [
                    {
                        "functionDeclarations": [
                            {
                                "name": "get_current_weather",
                                "description": "Get the current weather in a given location",
                                "parameters": {
                                    "type": "object",
                                    "description": "a description"
                                }
                            }
                        ]
                    }
                ]
            }
            """;
        assertJsonEquals(jsonString, expectedJson);
    }

    public void testSerialization_ToolsChoice() throws IOException {
        var request = new UnifiedCompletionRequest(
            List.of(
                new UnifiedCompletionRequest.Message(
                    new UnifiedCompletionRequest.ContentObjects(List.of(new UnifiedCompletionRequest.ContentObject("some text", "text"))),
                    "user",
                    null,
                    null
                )
            ),
            "gemini-2.0",
            null,
            null,
            null,
            new UnifiedCompletionRequest.ToolChoiceObject(
                "function",
                new UnifiedCompletionRequest.ToolChoiceObject.FunctionField("some function")
            ),
            List.of(
                new UnifiedCompletionRequest.Tool(
                    "function",
                    new UnifiedCompletionRequest.Tool.FunctionField(
                        "Get the current weather in a given location",
                        "get_current_weather",
                        Map.of("type", "object", "description", "a description"),
                        null
                    )
                )
            ),
            null
        );
        UnifiedChatInput unifiedChatInput = new UnifiedChatInput(request, false);

        GoogleVertexAiUnifiedChatCompletionRequestEntity entity = new GoogleVertexAiUnifiedChatCompletionRequestEntity(unifiedChatInput);

        XContentBuilder builder = JsonXContent.contentBuilder();
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String jsonString = Strings.toString(builder);
        String expectedJson = """
            {
                "contents": [
                    {
                        "role": "user",
                        "parts": [ { "text": "some text" } ]
                    }
                ],
                "tools": [
                    {
                        "functionDeclarations": [
                            {
                                "name": "get_current_weather",
                                "description": "Get the current weather in a given location",
                                "parameters": {
                                    "type": "object",
                                    "description": "a description"
                                }
                            }
                        ]
                    }
                ],
                "toolConfig": {
                    "functionCallingConfig" : {
                        "mode": "ANY",
                        "allowedFunctionNames": [ "some function" ]
                    }
                }
            }
            """;
        assertJsonEquals(jsonString, expectedJson);
    }

    public void testSerialization_WithAllGenerationConfig() throws IOException {
        List<UnifiedCompletionRequest.Message> messages = List.of(
            new UnifiedCompletionRequest.Message(new UnifiedCompletionRequest.ContentString("Hello Gemini!"), USER_ROLE, null, null)
        );
        var completionRequestWithGenerationConfig = new UnifiedCompletionRequest(
            messages,
            "modelId",
            100L,
            List.of("stop1", "stop2"),
            0.5f,
            null,
            null,
            0.9F
        );

        UnifiedChatInput unifiedChatInput = new UnifiedChatInput(completionRequestWithGenerationConfig, true);

        GoogleVertexAiUnifiedChatCompletionRequestEntity entity = new GoogleVertexAiUnifiedChatCompletionRequestEntity(unifiedChatInput);

        XContentBuilder builder = JsonXContent.contentBuilder();
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String jsonString = Strings.toString(builder);
        String expectedJson = """
            {
                "contents": [
                    {
                        "role": "user",
                        "parts": [ { "text": "Hello Gemini!" } ]
                    }
                ],
                "generationConfig": {
                    "stopSequences": ["stop1", "stop2"],
                    "temperature": 0.5,
                    "maxOutputTokens": 100,
                    "topP": 0.9
                }
            }
            """;
        assertJsonEquals(jsonString, expectedJson);
    }

    public void testSerialization_WithSomeGenerationConfig() throws IOException {
        UnifiedCompletionRequest.Message message = new UnifiedCompletionRequest.Message(
            new UnifiedCompletionRequest.ContentString("Partial config."),
            USER_ROLE,
            null,
            null
        );
        var completionRequestWithGenerationConfig = new UnifiedCompletionRequest(
            List.of(message),
            "modelId",
            50L,
            null,
            0.7f,
            null,
            null,
            null
        );

        UnifiedChatInput unifiedChatInput = new UnifiedChatInput(completionRequestWithGenerationConfig, true);

        GoogleVertexAiUnifiedChatCompletionRequestEntity entity = new GoogleVertexAiUnifiedChatCompletionRequestEntity(unifiedChatInput);

        XContentBuilder builder = JsonXContent.contentBuilder();
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String jsonString = Strings.toString(builder);
        String expectedJson = """
            {
                "contents": [
                    {
                        "role": "user",
                        "parts": [ { "text": "Partial config." } ]
                    }
                ],
                "generationConfig": {
                    "temperature": 0.7,
                    "maxOutputTokens": 50
                }
            }
            """;
        assertJsonEquals(jsonString, expectedJson);
    }

    public void testSerialization_NoGenerationConfig() throws IOException {
        UnifiedCompletionRequest.Message message = new UnifiedCompletionRequest.Message(
            new UnifiedCompletionRequest.ContentString("No extra config."),
            USER_ROLE,
            null,
            null
        );
        // No generation config fields set on unifiedRequest
        var unifiedRequest = UnifiedCompletionRequest.of(List.of(message));

        UnifiedChatInput unifiedChatInput = new UnifiedChatInput(unifiedRequest, true);

        GoogleVertexAiUnifiedChatCompletionRequestEntity entity = new GoogleVertexAiUnifiedChatCompletionRequestEntity(unifiedChatInput);

        XContentBuilder builder = JsonXContent.contentBuilder();
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String jsonString = Strings.toString(builder);
        String expectedJson = """
            {
                "contents": [
                    {
                        "role": "user",
                        "parts": [ { "text": "No extra config." } ]
                    }
                ]
            }
            """;
        assertJsonEquals(jsonString, expectedJson);
    }

    public void testSerialization_WithContentObjects() throws IOException {
        var contentObjects = List.of(
            new UnifiedCompletionRequest.ContentObject("First part. ", "text"),
            new UnifiedCompletionRequest.ContentObject("Second part.", "text")
        );
        UnifiedCompletionRequest.Message message = new UnifiedCompletionRequest.Message(
            new UnifiedCompletionRequest.ContentObjects(contentObjects),
            USER_ROLE,
            null,
            null
        );
        var messageList = new ArrayList<UnifiedCompletionRequest.Message>();
        messageList.add(message);

        var unifiedRequest = UnifiedCompletionRequest.of(messageList);
        UnifiedChatInput unifiedChatInput = new UnifiedChatInput(unifiedRequest, true);

        GoogleVertexAiUnifiedChatCompletionRequestEntity entity = new GoogleVertexAiUnifiedChatCompletionRequestEntity(unifiedChatInput);

        XContentBuilder builder = JsonXContent.contentBuilder();
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String jsonString = Strings.toString(builder);
        String expectedJson = """
            {
                "contents": [
                    {
                        "role": "user",
                        "parts": [
                            { "text": "First part. " },
                            { "text": "Second part." }
                        ]
                    }
                ]
            }
            """;
        assertJsonEquals(jsonString, expectedJson);
    }

    public void testError_UnsupportedRole() throws IOException {
        var unsupportedRole = "someUnexpectedRole";
        UnifiedCompletionRequest.Message message = new UnifiedCompletionRequest.Message(
            new UnifiedCompletionRequest.ContentString("Test"),
            unsupportedRole,
            null,
            null
        );
        var unifiedRequest = UnifiedCompletionRequest.of(List.of(message));
        UnifiedChatInput unifiedChatInput = new UnifiedChatInput(unifiedRequest, false);

        GoogleVertexAiUnifiedChatCompletionRequestEntity entity = new GoogleVertexAiUnifiedChatCompletionRequestEntity(unifiedChatInput);

        var builder = JsonXContent.contentBuilder();
        var statusException = assertThrows(ElasticsearchStatusException.class, () -> entity.toXContent(builder, ToXContent.EMPTY_PARAMS));

        assertEquals(RestStatus.BAD_REQUEST, statusException.status());
        var errorMessage = Strings.format("Role [%s] not supported by Google VertexAI ChatCompletion", unsupportedRole);
        assertThat(statusException.toString(), containsString(errorMessage));
    }

    public void testError_UnsupportedContentObjectType() throws IOException {
        var contentObjects = List.of(new UnifiedCompletionRequest.ContentObject("http://example.com/image.png", "image_url"));
        UnifiedCompletionRequest.Message message = new UnifiedCompletionRequest.Message(
            new UnifiedCompletionRequest.ContentObjects(contentObjects),
            USER_ROLE,
            null,
            null
        );
        var unifiedRequest = UnifiedCompletionRequest.of(List.of(message));
        UnifiedChatInput unifiedChatInput = new UnifiedChatInput(unifiedRequest, false);

        GoogleVertexAiUnifiedChatCompletionRequestEntity entity = new GoogleVertexAiUnifiedChatCompletionRequestEntity(unifiedChatInput);

        var builder = JsonXContent.contentBuilder();
        var statusException = assertThrows(ElasticsearchStatusException.class, () -> entity.toXContent(builder, ToXContent.EMPTY_PARAMS));

        assertEquals(RestStatus.BAD_REQUEST, statusException.status());
        assertThat(statusException.toString(), containsString("Type [image_url] not supported by Google VertexAI ChatCompletion"));
    }

    public void testParseAllFields() throws IOException {
        String requestJson = """
            {
              "contents": [
                {
                  "role": "user",
                  "parts": [
                    {
                      "text": "some text"
                    },
                    {
                      "functionCall": {
                        "name": "get_delivery_date",
                        "args": {
                          "order_id": "order_12345"
                        }
                      }
                    }
                  ]
                }
              ],
              "generationConfig": {
                "stopSequences": [
                  "stop"
                ],
                "temperature": 0.1,
                "maxOutputTokens": 100,
                "topP": 0.2
              },
              "tools": [
                {
                  "functionDeclarations": [
                    {
                      "name": "get_current_weather",
                      "description": "Get the current weather in a given location",
                      "parameters": {
                        "type": "object"
                      }
                    }
                  ]
                }
              ],
              "toolConfig": {
                "functionCallingConfig": {
                  "mode": "ANY",
                  "allowedFunctionNames": [
                    "some function"
                  ]
                }
              }
            }
            """;

        var request = new UnifiedCompletionRequest(
            List.of(
                new UnifiedCompletionRequest.Message(
                    new UnifiedCompletionRequest.ContentObjects(List.of(new UnifiedCompletionRequest.ContentObject("some text", "text"))),
                    "user",
                    "100",
                    List.of(
                        new UnifiedCompletionRequest.ToolCall(
                            "call_62136354",
                            new UnifiedCompletionRequest.ToolCall.FunctionField("{\"order_id\": \"order_12345\"}", "get_delivery_date"),
                            "function"
                        )
                    )
                )
            ),
            "gemini-2.0",
            100L,
            List.of("stop"),
            0.1F,
            new UnifiedCompletionRequest.ToolChoiceObject(
                "function",
                new UnifiedCompletionRequest.ToolChoiceObject.FunctionField("some function")
            ),
            List.of(
                new UnifiedCompletionRequest.Tool(
                    "function",
                    new UnifiedCompletionRequest.Tool.FunctionField(
                        "Get the current weather in a given location",
                        "get_current_weather",
                        Map.of("type", "object"),
                        null
                    )
                )
            ),
            0.2F
        );

        UnifiedChatInput unifiedChatInput = new UnifiedChatInput(request, true);
        GoogleVertexAiUnifiedChatCompletionRequestEntity entity = new GoogleVertexAiUnifiedChatCompletionRequestEntity(unifiedChatInput);

        XContentBuilder builder = JsonXContent.contentBuilder();
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String jsonString = Strings.toString(builder);
        assertJsonEquals(jsonString, requestJson);
    }

    public void testParseFunctionCallNoContent() throws IOException {
        String requestJson = """
            {
                "contents": [
                    {
                        "role": "model",
                        "parts": [
                            { "functionCall" : {
                                "name": "get_delivery_date",
                                "args": {
                                    "order_id" : "order_12345"
                                    }
                                }
                            }
                        ]
                    }
                ]
            }
            """;

        var request = new UnifiedCompletionRequest(
            List.of(
                new UnifiedCompletionRequest.Message(
                    null,
                    "tool",
                    "100",
                    List.of(
                        new UnifiedCompletionRequest.ToolCall(
                            "call_62136354",
                            new UnifiedCompletionRequest.ToolCall.FunctionField("{\"order_id\": \"order_12345\"}", "get_delivery_date"),
                            "function"
                        )
                    )
                )
            ),
            "gemini-2.0",
            null,
            null,
            null,
            null,
            null,
            null
        );

        UnifiedChatInput unifiedChatInput = new UnifiedChatInput(request, true);
        GoogleVertexAiUnifiedChatCompletionRequestEntity entity = new GoogleVertexAiUnifiedChatCompletionRequestEntity(unifiedChatInput);

        XContentBuilder builder = JsonXContent.contentBuilder();
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String jsonString = Strings.toString(builder);
        assertJsonEquals(jsonString, requestJson);
    }

    public void testParseFunctionCallWithBadJson() throws IOException {
        int someNumber = 1;
        var illegalArguments = List.of("\"order_id\": \"order_12345\"}", "[]", Integer.toString(someNumber), "\"a\"");
        for (var illegalArgument : illegalArguments) {

            var requestContentObject = new UnifiedCompletionRequest(
                List.of(
                    new UnifiedCompletionRequest.Message(
                        new UnifiedCompletionRequest.ContentObjects(List.of(new UnifiedCompletionRequest.ContentObject("", "text"))),
                        "assistant",
                        null,
                        List.of(
                            new UnifiedCompletionRequest.ToolCall(
                                "call_62136354",
                                new UnifiedCompletionRequest.ToolCall.FunctionField(illegalArgument, "get_delivery_date"),
                                "function"
                            )
                        )
                    )
                ),
                "gemini-2.0",
                null,
                null,
                null,
                null,
                null,
                null
            );

            UnifiedChatInput unifiedChatInput = new UnifiedChatInput(requestContentObject, true);
            GoogleVertexAiUnifiedChatCompletionRequestEntity entity = new GoogleVertexAiUnifiedChatCompletionRequestEntity(
                unifiedChatInput
            );

            XContentBuilder builder = JsonXContent.contentBuilder();

            assertThrows(ParsingException.class, () -> entity.toXContent(builder, ToXContent.EMPTY_PARAMS));
        }

    }

    public void testParseFunctionCallWithEmptyStringContent() throws IOException {
        String requestJson = """
            {
                "contents": [
                    {
                        "role": "model",
                        "parts": [
                            { "functionCall" : {
                                "name": "get_delivery_date",
                                "args": {
                                    "order_id" : "order_12345"
                                    }
                                }
                            }
                        ]
                    }
                ]
            }
            """;

        var requestContentObject = new UnifiedCompletionRequest(
            List.of(
                new UnifiedCompletionRequest.Message(
                    new UnifiedCompletionRequest.ContentObjects(List.of(new UnifiedCompletionRequest.ContentObject("", "text"))),
                    "assistant",
                    null,
                    List.of(
                        new UnifiedCompletionRequest.ToolCall(
                            "call_62136354",
                            new UnifiedCompletionRequest.ToolCall.FunctionField("{\"order_id\": \"order_12345\"}", "get_delivery_date"),
                            "function"
                        )
                    )
                )
            ),
            "gemini-2.0",
            null,
            null,
            null,
            null,
            null,
            null
        );

        var requestContentString = new UnifiedCompletionRequest(
            List.of(
                new UnifiedCompletionRequest.Message(
                    new UnifiedCompletionRequest.ContentString(""),
                    "assistant",
                    null,
                    List.of(
                        new UnifiedCompletionRequest.ToolCall(
                            "call_62136354",
                            new UnifiedCompletionRequest.ToolCall.FunctionField("{\"order_id\": \"order_12345\"}", "get_delivery_date"),
                            "function"
                        )
                    )
                )
            ),
            "gemini-2.0",
            null,
            null,
            null,
            null,
            null,
            null
        );
        var requests = List.of(requestContentObject, requestContentString);

        for (var request : requests) {
            UnifiedChatInput unifiedChatInput = new UnifiedChatInput(request, true);
            GoogleVertexAiUnifiedChatCompletionRequestEntity entity = new GoogleVertexAiUnifiedChatCompletionRequestEntity(
                unifiedChatInput
            );

            XContentBuilder builder = JsonXContent.contentBuilder();
            entity.toXContent(builder, ToXContent.EMPTY_PARAMS);

            String jsonString = Strings.toString(builder);
            assertJsonEquals(jsonString, requestJson);
        }
    }

    public void testParseToolChoiceString() throws IOException {
        String requestJson = """
            {
                "contents": [
                    {
                        "role": "user",
                        "parts": [
                            { "text": "some text" }
                        ]
                    }
                ]
            }
            """;

        var request = new UnifiedCompletionRequest(
            List.of(
                new UnifiedCompletionRequest.Message(
                    new UnifiedCompletionRequest.ContentObjects(List.of(new UnifiedCompletionRequest.ContentObject("some text", "text"))),
                    "user",
                    null,
                    null
                )
            ),
            "gemini-2.0",
            null,
            null,
            null,
            new UnifiedCompletionRequest.ToolChoiceString("auto"),
            null,
            null
        );

        UnifiedChatInput unifiedChatInput = new UnifiedChatInput(request, true);
        GoogleVertexAiUnifiedChatCompletionRequestEntity entity = new GoogleVertexAiUnifiedChatCompletionRequestEntity(unifiedChatInput);

        XContentBuilder builder = JsonXContent.contentBuilder();
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String jsonString = Strings.toString(builder);
        assertJsonEquals(jsonString, requestJson);
    }

    public void testBuildSystemMessage_MultipleParts() throws IOException {
        String requestJson = """
            {
                "systemInstruction": {
                        "parts": [
                            { "text": "instruction text" },
                            { "text": "instruction text2" }
                        ]
                    },
                "contents": [
                    {
                        "role": "user",
                        "parts": [
                            { "text": "some text" }
                        ]
                    }
                ]
            }
            """;

        var request = new UnifiedCompletionRequest(
            List.of(
                new UnifiedCompletionRequest.Message(
                    new UnifiedCompletionRequest.ContentObjects(
                        List.of(new UnifiedCompletionRequest.ContentObject("instruction text", "text"))
                    ),
                    "system",
                    null,
                    null
                ),
                new UnifiedCompletionRequest.Message(
                    new UnifiedCompletionRequest.ContentObjects(
                        List.of(new UnifiedCompletionRequest.ContentObject("instruction text2", "text"))
                    ),
                    "system",
                    null,
                    null
                ),
                new UnifiedCompletionRequest.Message(
                    new UnifiedCompletionRequest.ContentObjects(List.of(new UnifiedCompletionRequest.ContentObject("some text", "text"))),
                    "user",
                    null,
                    null
                )
            ),
            "gemini-2.0",
            null,
            null,
            null,
            new UnifiedCompletionRequest.ToolChoiceString("auto"),
            null,
            null
        );

        UnifiedChatInput unifiedChatInput = new UnifiedChatInput(request, true);
        GoogleVertexAiUnifiedChatCompletionRequestEntity entity = new GoogleVertexAiUnifiedChatCompletionRequestEntity(unifiedChatInput);

        XContentBuilder builder = JsonXContent.contentBuilder();
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String jsonString = Strings.toString(builder);
        assertJsonEquals(jsonString, requestJson);
    }

    public void testBuildSystemMessageMul() throws IOException {
        String requestJson = """
            {
                "systemInstruction": {
                        "parts": [
                            { "text": "instruction text" }
                        ]
                    },
                "contents": [
                    {
                        "role": "user",
                        "parts": [
                            { "text": "some text" }
                        ]
                    }
                ]
            }
            """;

        var request = new UnifiedCompletionRequest(
            List.of(
                new UnifiedCompletionRequest.Message(
                    new UnifiedCompletionRequest.ContentObjects(
                        List.of(new UnifiedCompletionRequest.ContentObject("instruction text", "text"))
                    ),
                    "system",
                    null,
                    null
                ),
                new UnifiedCompletionRequest.Message(
                    new UnifiedCompletionRequest.ContentObjects(List.of(new UnifiedCompletionRequest.ContentObject("some text", "text"))),
                    "user",
                    null,
                    null
                )
            ),
            "gemini-2.0",
            null,
            null,
            null,
            new UnifiedCompletionRequest.ToolChoiceString("auto"),
            null,
            null
        );

        UnifiedChatInput unifiedChatInput = new UnifiedChatInput(request, true);
        GoogleVertexAiUnifiedChatCompletionRequestEntity entity = new GoogleVertexAiUnifiedChatCompletionRequestEntity(unifiedChatInput);

        XContentBuilder builder = JsonXContent.contentBuilder();
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String jsonString = Strings.toString(builder);
        assertJsonEquals(jsonString, requestJson);
    }

    public void testParseToolChoiceInvalid_throwElasticSearchStatusException() throws IOException {
        var request = new UnifiedCompletionRequest(
            List.of(
                new UnifiedCompletionRequest.Message(
                    new UnifiedCompletionRequest.ContentObjects(List.of(new UnifiedCompletionRequest.ContentObject("some text", "text"))),
                    "user",
                    null,
                    null
                )
            ),
            "gemini-2.0",
            null,
            null,
            null,
            new UnifiedCompletionRequest.ToolChoiceString("unsupported"),
            null,
            null
        );

        UnifiedChatInput unifiedChatInput = new UnifiedChatInput(request, true);
        GoogleVertexAiUnifiedChatCompletionRequestEntity entity = new GoogleVertexAiUnifiedChatCompletionRequestEntity(unifiedChatInput);

        XContentBuilder builder = JsonXContent.contentBuilder();
        var statusException = expectThrows(ElasticsearchStatusException.class, () -> entity.toXContent(builder, ToXContent.EMPTY_PARAMS));

        assertThat(
            statusException.toString(),
            containsString("Tool choice value [unsupported] not supported by Google VertexAI ChatCompletion.")
        );

    }

    public void testParseMultipleTools() throws IOException {
        String requestJson = """
            {
                "contents": [
                    {
                        "role": "user",
                        "parts": [
                            { "text": "some text" }
                        ]
                    }
                ],
                "tools": [
                    {
                        "functionDeclarations": [
                            {
                                "name": "get_current_weather",
                                "description": "Get the current weather in a given location",
                                "parameters": {
                                    "type": "object"
                                }
                            },
                            {
                                "name": "get_current_temperature",
                                "description": "Get the current temperature in a location",
                                "parameters": {
                                    "type": "object"
                                }
                            }
                        ]
                    }
                ]
            }
            """;

        var request = new UnifiedCompletionRequest(
            List.of(
                new UnifiedCompletionRequest.Message(
                    new UnifiedCompletionRequest.ContentObjects(List.of(new UnifiedCompletionRequest.ContentObject("some text", "text"))),
                    "user",
                    null,
                    null
                )
            ),
            "gemini-2.0",
            null,
            null,
            null,
            null,
            List.of(
                new UnifiedCompletionRequest.Tool(
                    "function",
                    new UnifiedCompletionRequest.Tool.FunctionField(
                        "Get the current weather in a given location",
                        "get_current_weather",
                        Map.of("type", "object"),
                        null
                    )
                ),
                new UnifiedCompletionRequest.Tool(
                    "function",
                    new UnifiedCompletionRequest.Tool.FunctionField(
                        "Get the current temperature in a location",
                        "get_current_temperature",
                        Map.of("type", "object"),
                        null
                    )
                )
            ),
            null
        );

        UnifiedChatInput unifiedChatInput = new UnifiedChatInput(request, true);
        GoogleVertexAiUnifiedChatCompletionRequestEntity entity = new GoogleVertexAiUnifiedChatCompletionRequestEntity(unifiedChatInput);

        XContentBuilder builder = JsonXContent.contentBuilder();
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String jsonString = Strings.toString(builder);
        assertJsonEquals(jsonString, requestJson);
    }
}
