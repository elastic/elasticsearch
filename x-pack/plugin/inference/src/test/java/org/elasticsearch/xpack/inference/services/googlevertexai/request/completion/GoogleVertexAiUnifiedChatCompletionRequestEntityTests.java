/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.request.completion;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.UnifiedCompletionRequestBody;
import org.elasticsearch.inference.completion.ContentObject;
import org.elasticsearch.inference.completion.ContentObject.ContentObjectText;
import org.elasticsearch.inference.completion.ContentObjects;
import org.elasticsearch.inference.completion.ContentString;
import org.elasticsearch.inference.completion.Message;
import org.elasticsearch.inference.completion.Reasoning;
import org.elasticsearch.inference.completion.Reasoning.ReasoningEffort;
import org.elasticsearch.inference.completion.ReasoningDetail.TextReasoningDetail;
import org.elasticsearch.inference.completion.Tool;
import org.elasticsearch.inference.completion.ToolCall;
import org.elasticsearch.inference.completion.ToolChoice.ToolChoiceObject;
import org.elasticsearch.inference.completion.ToolChoice.ToolChoiceString;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.googlevertexai.completion.ThinkingConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.Utils.assertJsonEquals;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class GoogleVertexAiUnifiedChatCompletionRequestEntityTests extends ESTestCase {

    private static final String USER_ROLE = "user";
    private static final String ASSISTANT_ROLE = "assistant";
    private static final String TOOL_ROLE = "tool";
    private static final ThinkingConfig thinkingConfig = new ThinkingConfig(256);
    private static final ThinkingConfig emptyThinkingConfig = new ThinkingConfig();

    private static final String FUNCTION_NAME = "get_delivery_date";
    private static final String FUNCTION_ARGUMENTS = "{\"order_id\": \"order_12345\"}";
    private static final String GOOGLE_TOOL_CALL_ID = "call_299965";
    private static final String SECOND_TOOL_CALL_ID = "call_second";
    private static final String SECOND_FUNCTION_NAME = "get_eta";
    private static final String THOUGHT_SIGNATURE = "El4KXAERTTIPHPmb/yri/Qyy9cz7xqWoMPh394Dk3bIAt2jgXMJoP2cOWRyqxOs";
    private static final String REASONING_FORMAT = "google-vertex-ai-v1";
    private static final String TOOL_RESULT_JSON = "{\"delivery_date\": \"2025-03-27\"}";

    public void testBasicSerialization_SingleMessage() throws IOException {
        Message message = new Message(new ContentString("Hello, Vertex AI!"), USER_ROLE, null, null);
        var messageList = new ArrayList<Message>();
        messageList.add(message);

        var unifiedRequest = UnifiedCompletionRequestBody.of(messageList);
        UnifiedChatInput unifiedChatInput = new UnifiedChatInput(unifiedRequest, true); // stream doesn't affect VertexAI request body
        GoogleVertexAiUnifiedChatCompletionRequestEntity entity = new GoogleVertexAiUnifiedChatCompletionRequestEntity(
            unifiedChatInput,
            emptyThinkingConfig
        );

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
            new Message(new ContentString("Previous user message."), USER_ROLE, null, null),
            new Message(new ContentString("Previous model response."), ASSISTANT_ROLE, null, null),
            new Message(new ContentString("Current user query."), USER_ROLE, null, null)
        );

        var unifiedRequest = UnifiedCompletionRequestBody.of(messages);
        UnifiedChatInput unifiedChatInput = new UnifiedChatInput(unifiedRequest, false);

        GoogleVertexAiUnifiedChatCompletionRequestEntity entity = new GoogleVertexAiUnifiedChatCompletionRequestEntity(
            unifiedChatInput,
            emptyThinkingConfig
        );

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
        var request = new UnifiedCompletionRequestBody(
            List.of(new Message(new ContentObjects(List.of(new ContentObjectText("some text"))), "user", null, null)),
            "gemini-2.0",
            null,
            null,
            null,
            null,
            List.of(
                new Tool(
                    "function",
                    new Tool.FunctionField(
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

        GoogleVertexAiUnifiedChatCompletionRequestEntity entity = new GoogleVertexAiUnifiedChatCompletionRequestEntity(
            unifiedChatInput,
            emptyThinkingConfig
        );

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
        var request = new UnifiedCompletionRequestBody(
            List.of(new Message(new ContentObjects(List.of(new ContentObjectText("some text"))), "user", null, null)),
            "gemini-2.0",
            null,
            null,
            null,
            new ToolChoiceObject("function", new ToolChoiceObject.FunctionField("some function")),
            List.of(
                new Tool(
                    "function",
                    new Tool.FunctionField(
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

        GoogleVertexAiUnifiedChatCompletionRequestEntity entity = new GoogleVertexAiUnifiedChatCompletionRequestEntity(
            unifiedChatInput,
            emptyThinkingConfig
        );

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
        List<Message> messages = List.of(new Message(new ContentString("Hello Gemini!"), USER_ROLE, null, null));
        var completionRequestWithGenerationConfig = new UnifiedCompletionRequestBody(
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

        GoogleVertexAiUnifiedChatCompletionRequestEntity entity = new GoogleVertexAiUnifiedChatCompletionRequestEntity(
            unifiedChatInput,
            thinkingConfig
        );

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
                    "topP": 0.9,
                    "thinkingConfig": {
                      "thinkingBudget": 256
                    }
                }
            }
            """;
        assertJsonEquals(jsonString, expectedJson);
    }

    public void testSerialization_WithSomeGenerationConfig() throws IOException {
        Message message = new Message(new ContentString("Partial config."), USER_ROLE, null, null);
        var completionRequestWithGenerationConfig = new UnifiedCompletionRequestBody(
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

        GoogleVertexAiUnifiedChatCompletionRequestEntity entity = new GoogleVertexAiUnifiedChatCompletionRequestEntity(
            unifiedChatInput,
            emptyThinkingConfig
        );

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

    public void testSerialization_WithOnlyThinkingConfig() throws IOException {
        Message message = new Message(new ContentString("Partial config."), USER_ROLE, null, null);

        // No generation config fields set on unifiedRequest
        var completionRequestWithNoGenerationConfig = UnifiedCompletionRequestBody.of(List.of(message));

        UnifiedChatInput unifiedChatInput = new UnifiedChatInput(completionRequestWithNoGenerationConfig, true);

        GoogleVertexAiUnifiedChatCompletionRequestEntity entity = new GoogleVertexAiUnifiedChatCompletionRequestEntity(
            unifiedChatInput,
            thinkingConfig
        );

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
                    "thinkingConfig": {
                      "thinkingBudget": 256
                    }
                }
            }
            """;
        assertJsonEquals(jsonString, expectedJson);
    }

    public void testSerialization_FallsBackToTaskSettingsMaxTokensWhenRequestValueIsNull() throws IOException {
        // The max_tokens task setting must be honoured when the per-request maxCompletionTokens is null.
        Message message = new Message(new ContentString("Use my task setting."), USER_ROLE, null, null);
        var unifiedRequest = UnifiedCompletionRequestBody.of(List.of(message));
        UnifiedChatInput unifiedChatInput = new UnifiedChatInput(unifiedRequest, true);

        GoogleVertexAiUnifiedChatCompletionRequestEntity entity = new GoogleVertexAiUnifiedChatCompletionRequestEntity(
            unifiedChatInput,
            emptyThinkingConfig,
            42
        );

        XContentBuilder builder = JsonXContent.contentBuilder();
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String expectedJson = """
            {
                "contents": [
                    {
                        "role": "user",
                        "parts": [ { "text": "Use my task setting." } ]
                    }
                ],
                "generationConfig": {
                    "maxOutputTokens": 42
                }
            }
            """;
        assertJsonEquals(Strings.toString(builder), expectedJson);
    }

    public void testSerialization_RequestMaxCompletionTokensTakesPrecedenceOverTaskSettings() throws IOException {
        Message message = new Message(new ContentString("Use the request value."), USER_ROLE, null, null);
        var unifiedRequest = new UnifiedCompletionRequestBody(
            List.of(message),
            "modelId",
            123L, // explicit per-request value
            null,
            null,
            null,
            null,
            null
        );
        UnifiedChatInput unifiedChatInput = new UnifiedChatInput(unifiedRequest, true);

        GoogleVertexAiUnifiedChatCompletionRequestEntity entity = new GoogleVertexAiUnifiedChatCompletionRequestEntity(
            unifiedChatInput,
            emptyThinkingConfig,
            42 // would be used if request value were null
        );

        XContentBuilder builder = JsonXContent.contentBuilder();
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String expectedJson = """
            {
                "contents": [
                    {
                        "role": "user",
                        "parts": [ { "text": "Use the request value." } ]
                    }
                ],
                "generationConfig": {
                    "maxOutputTokens": 123
                }
            }
            """;
        assertJsonEquals(Strings.toString(builder), expectedJson);
    }

    public void testSerialization_NoGenerationConfig() throws IOException {
        Message message = new Message(new ContentString("No extra config."), USER_ROLE, null, null);
        // No generation config fields set on unifiedRequest
        var unifiedRequest = UnifiedCompletionRequestBody.of(List.of(message));

        UnifiedChatInput unifiedChatInput = new UnifiedChatInput(unifiedRequest, true);

        GoogleVertexAiUnifiedChatCompletionRequestEntity entity = new GoogleVertexAiUnifiedChatCompletionRequestEntity(
            unifiedChatInput,
            emptyThinkingConfig
        );

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
        List<ContentObject> contentObjects = List.of(new ContentObjectText("First part. "), new ContentObjectText("Second part."));
        Message message = new Message(new ContentObjects(contentObjects), USER_ROLE, null, null);
        var messageList = new ArrayList<Message>();
        messageList.add(message);

        var unifiedRequest = UnifiedCompletionRequestBody.of(messageList);
        UnifiedChatInput unifiedChatInput = new UnifiedChatInput(unifiedRequest, true);

        GoogleVertexAiUnifiedChatCompletionRequestEntity entity = new GoogleVertexAiUnifiedChatCompletionRequestEntity(
            unifiedChatInput,
            emptyThinkingConfig
        );

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
        Message message = new Message(new ContentString("Test"), unsupportedRole, null, null);
        var unifiedRequest = UnifiedCompletionRequestBody.of(List.of(message));
        UnifiedChatInput unifiedChatInput = new UnifiedChatInput(unifiedRequest, false);

        GoogleVertexAiUnifiedChatCompletionRequestEntity entity = new GoogleVertexAiUnifiedChatCompletionRequestEntity(
            unifiedChatInput,
            emptyThinkingConfig
        );

        var builder = JsonXContent.contentBuilder();
        var statusException = assertThrows(ElasticsearchStatusException.class, () -> entity.toXContent(builder, ToXContent.EMPTY_PARAMS));

        assertEquals(RestStatus.BAD_REQUEST, statusException.status());
        var errorMessage = Strings.format("Role [%s] not supported by Google VertexAI ChatCompletion", unsupportedRole);
        assertThat(statusException.toString(), containsString(errorMessage));
    }

    public void testError_UnsupportedContentObjectType() throws IOException {
        List<ContentObject> contentObjects = List.of(
            new ContentObject.ContentObjectImage(
                new ContentObject.ContentObjectImage.ContentObjectImageUrl("http://example.com/image.png", null)
            )
        );
        Message message = new Message(new ContentObjects(contentObjects), USER_ROLE, null, null);
        var unifiedRequest = UnifiedCompletionRequestBody.of(List.of(message));
        UnifiedChatInput unifiedChatInput = new UnifiedChatInput(unifiedRequest, false);

        GoogleVertexAiUnifiedChatCompletionRequestEntity entity = new GoogleVertexAiUnifiedChatCompletionRequestEntity(
            unifiedChatInput,
            emptyThinkingConfig
        );

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
                        },
                        "id": "call_62136354"
                      },
                      "thoughtSignature": "skip_thought_signature_validator"
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
                "topP": 0.2,
                "thinkingConfig": {
                  "thinkingBudget": 256
                }
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

        var request = new UnifiedCompletionRequestBody(
            List.of(
                new Message(
                    new ContentObjects(List.of(new ContentObjectText("some text"))),
                    "user",
                    "100",
                    List.of(
                        new ToolCall(
                            "call_62136354",
                            new ToolCall.FunctionField("{\"order_id\": \"order_12345\"}", "get_delivery_date"),
                            "function"
                        )
                    )
                )
            ),
            "gemini-2.0",
            100L,
            List.of("stop"),
            0.1F,
            new ToolChoiceObject("function", new ToolChoiceObject.FunctionField("some function")),
            List.of(
                new Tool(
                    "function",
                    new Tool.FunctionField(
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
        GoogleVertexAiUnifiedChatCompletionRequestEntity entity = new GoogleVertexAiUnifiedChatCompletionRequestEntity(
            unifiedChatInput,
            thinkingConfig
        );

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
                            {
                                "functionCall" : {
                                    "name": "get_delivery_date",
                                    "args": {
                                        "order_id" : "order_12345"
                                    },
                                    "id": "call_62136354"
                                },
                                "thoughtSignature": "skip_thought_signature_validator"
                            }
                        ]
                    }
                ]
            }
            """;

        // A message carrying tool calls is an assistant turn; a tool-role message carries the result of one and is
        // covered by the functionResponse tests.
        var request = new UnifiedCompletionRequestBody(
            List.of(
                new Message(
                    null,
                    ASSISTANT_ROLE,
                    null,
                    List.of(
                        new ToolCall(
                            "call_62136354",
                            new ToolCall.FunctionField("{\"order_id\": \"order_12345\"}", "get_delivery_date"),
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
        GoogleVertexAiUnifiedChatCompletionRequestEntity entity = new GoogleVertexAiUnifiedChatCompletionRequestEntity(
            unifiedChatInput,
            emptyThinkingConfig
        );

        XContentBuilder builder = JsonXContent.contentBuilder();
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String jsonString = Strings.toString(builder);
        assertJsonEquals(jsonString, requestJson);
    }

    public void testParseFunctionCallWithNonStringArgValues() throws IOException {
        String requestJson = """
            {
                "contents": [
                    {
                        "role": "model",
                        "parts": [
                            {
                                "functionCall" : {
                                    "name": "get_index_mapping",
                                    "args": {
                                        "indices": ["foo", "bar"],
                                        "size": 10
                                    },
                                    "id": "call_1"
                                },
                                "thoughtSignature": "skip_thought_signature_validator"
                            }
                        ]
                    }
                ]
            }
            """;

        var request = new UnifiedCompletionRequestBody(
            List.of(
                new Message(
                    null,
                    "assistant",
                    null,
                    List.of(
                        new ToolCall(
                            "call_1",
                            new ToolCall.FunctionField("{\"indices\": [\"foo\", \"bar\"], \"size\": 10}", "get_index_mapping"),
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
        GoogleVertexAiUnifiedChatCompletionRequestEntity entity = new GoogleVertexAiUnifiedChatCompletionRequestEntity(
            unifiedChatInput,
            emptyThinkingConfig
        );

        XContentBuilder builder = JsonXContent.contentBuilder();
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String jsonString = Strings.toString(builder);
        assertJsonEquals(jsonString, requestJson);
    }

    public void testParseFunctionCallWithBadJson() throws IOException {
        int someNumber = 1;
        var illegalArguments = List.of("\"order_id\": \"order_12345\"}", "[]", Integer.toString(someNumber), "\"a\"");
        for (var illegalArgument : illegalArguments) {

            var requestContentObject = new UnifiedCompletionRequestBody(
                List.of(
                    new Message(
                        new ContentObjects(List.of(new ContentObjectText(""))),
                        "assistant",
                        null,
                        List.of(new ToolCall("call_62136354", new ToolCall.FunctionField(illegalArgument, "get_delivery_date"), "function"))
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
                unifiedChatInput,
                emptyThinkingConfig
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
                            {
                                "functionCall" : {
                                    "name": "get_delivery_date",
                                    "args": {
                                        "order_id" : "order_12345"
                                    },
                                    "id": "call_62136354"
                                },
                                "thoughtSignature": "skip_thought_signature_validator"
                            }
                        ]
                    }
                ]
            }
            """;

        var requestContentObject = new UnifiedCompletionRequestBody(
            List.of(
                new Message(
                    new ContentObjects(List.of(new ContentObjectText(""))),
                    "assistant",
                    null,
                    List.of(
                        new ToolCall(
                            "call_62136354",
                            new ToolCall.FunctionField("{\"order_id\": \"order_12345\"}", "get_delivery_date"),
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

        var requestContentString = new UnifiedCompletionRequestBody(
            List.of(
                new Message(
                    new ContentString(""),
                    "assistant",
                    null,
                    List.of(
                        new ToolCall(
                            "call_62136354",
                            new ToolCall.FunctionField("{\"order_id\": \"order_12345\"}", "get_delivery_date"),
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
                unifiedChatInput,
                emptyThinkingConfig
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

        var request = new UnifiedCompletionRequestBody(
            List.of(new Message(new ContentObjects(List.of(new ContentObjectText("some text"))), "user", null, null)),
            "gemini-2.0",
            null,
            null,
            null,
            new ToolChoiceString("auto"),
            null,
            null
        );

        UnifiedChatInput unifiedChatInput = new UnifiedChatInput(request, true);
        GoogleVertexAiUnifiedChatCompletionRequestEntity entity = new GoogleVertexAiUnifiedChatCompletionRequestEntity(
            unifiedChatInput,
            emptyThinkingConfig
        );

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

        var request = new UnifiedCompletionRequestBody(
            List.of(
                new Message(new ContentObjects(List.of(new ContentObjectText("instruction text"))), "system", null, null),
                new Message(new ContentObjects(List.of(new ContentObjectText("instruction text2"))), "system", null, null),
                new Message(new ContentObjects(List.of(new ContentObjectText("some text"))), "user", null, null)
            ),
            "gemini-2.0",
            null,
            null,
            null,
            new ToolChoiceString("auto"),
            null,
            null
        );

        UnifiedChatInput unifiedChatInput = new UnifiedChatInput(request, true);
        GoogleVertexAiUnifiedChatCompletionRequestEntity entity = new GoogleVertexAiUnifiedChatCompletionRequestEntity(
            unifiedChatInput,
            emptyThinkingConfig
        );

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

        var request = new UnifiedCompletionRequestBody(
            List.of(
                new Message(new ContentObjects(List.of(new ContentObjectText("instruction text"))), "system", null, null),
                new Message(new ContentObjects(List.of(new ContentObjectText("some text"))), "user", null, null)
            ),
            "gemini-2.0",
            null,
            null,
            null,
            new ToolChoiceString("auto"),
            null,
            null
        );

        UnifiedChatInput unifiedChatInput = new UnifiedChatInput(request, true);
        GoogleVertexAiUnifiedChatCompletionRequestEntity entity = new GoogleVertexAiUnifiedChatCompletionRequestEntity(
            unifiedChatInput,
            emptyThinkingConfig
        );

        XContentBuilder builder = JsonXContent.contentBuilder();
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String jsonString = Strings.toString(builder);
        assertJsonEquals(jsonString, requestJson);
    }

    public void testParseToolChoiceInvalid_throwElasticSearchStatusException() throws IOException {
        var request = new UnifiedCompletionRequestBody(
            List.of(new Message(new ContentObjects(List.of(new ContentObjectText("some text"))), "user", null, null)),
            "gemini-2.0",
            null,
            null,
            null,
            new ToolChoiceString("unsupported"),
            null,
            null
        );

        UnifiedChatInput unifiedChatInput = new UnifiedChatInput(request, true);
        GoogleVertexAiUnifiedChatCompletionRequestEntity entity = new GoogleVertexAiUnifiedChatCompletionRequestEntity(
            unifiedChatInput,
            emptyThinkingConfig
        );

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

        var request = new UnifiedCompletionRequestBody(
            List.of(new Message(new ContentObjects(List.of(new ContentObjectText("some text"))), "user", null, null)),
            "gemini-2.0",
            null,
            null,
            null,
            null,
            List.of(
                new Tool(
                    "function",
                    new Tool.FunctionField(
                        "Get the current weather in a given location",
                        "get_current_weather",
                        Map.of("type", "object"),
                        null
                    )
                ),
                new Tool(
                    "function",
                    new Tool.FunctionField(
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
        GoogleVertexAiUnifiedChatCompletionRequestEntity entity = new GoogleVertexAiUnifiedChatCompletionRequestEntity(
            unifiedChatInput,
            emptyThinkingConfig
        );

        XContentBuilder builder = JsonXContent.contentBuilder();
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String jsonString = Strings.toString(builder);
        assertJsonEquals(jsonString, requestJson);
    }

    public void testSerialization_ReasoningEffortMapsToThinkingLevel() throws IOException {
        assertThinkingLevel(ReasoningEffort.MINIMAL, "MINIMAL");
        assertThinkingLevel(ReasoningEffort.LOW, "LOW");
        assertThinkingLevel(ReasoningEffort.MEDIUM, "MEDIUM");
        assertThinkingLevel(ReasoningEffort.HIGH, "HIGH");
    }

    private void assertThinkingLevel(ReasoningEffort effort, String expectedThinkingLevel) throws IOException {
        var request = requestWithReasoning(new Reasoning(effort, null, null, null));

        assertJsonEquals(serialize(request, emptyThinkingConfig), Strings.format("""
            {
                "contents": [ { "role": "user", "parts": [ { "text": "Hello, Vertex AI!" } ] } ],
                "generationConfig": {
                    "thinkingConfig": { "thinkingLevel": "%s", "includeThoughts": true }
                }
            }
            """, expectedThinkingLevel));
    }

    public void testSerialization_ReasoningEffortXHigh_ThrowsBadRequest() {
        var request = requestWithReasoning(new Reasoning(ReasoningEffort.XHIGH, null, null, null));

        var exception = expectThrows(ElasticsearchStatusException.class, () -> serialize(request, emptyThinkingConfig));
        assertThat(exception.status(), is(RestStatus.BAD_REQUEST));
        assertThat(exception.getMessage(), containsString("Reasoning effort [xhigh] not supported"));
        assertThat(exception.getMessage(), containsString("minimal, low, medium, high"));
    }

    public void testSerialization_ReasoningEffortNone_ThrowsBadRequest() {
        // [enabled: false] requires an effort, so [none] is how a caller asks for thinking to be turned off.
        var request = requestWithReasoning(new Reasoning(ReasoningEffort.NONE, null, null, false));

        var exception = expectThrows(ElasticsearchStatusException.class, () -> serialize(request, emptyThinkingConfig));
        assertThat(exception.status(), is(RestStatus.BAD_REQUEST));
        assertThat(exception.getMessage(), containsString("Reasoning effort [none] not supported"));
    }

    public void testSerialization_RequestReasoningSuppressesTaskSettingsThinkingBudget() throws IOException {
        // Google rejects a request carrying both thinkingLevel and thinkingBudget, so the endpoint-level budget is
        // dropped when the request asks for reasoning of its own.
        var request = requestWithReasoning(new Reasoning(ReasoningEffort.LOW, null, null, null));

        assertJsonEquals(serialize(request, thinkingConfig), """
            {
                "contents": [ { "role": "user", "parts": [ { "text": "Hello, Vertex AI!" } ] } ],
                "generationConfig": {
                    "thinkingConfig": { "thinkingLevel": "LOW", "includeThoughts": true }
                }
            }
            """);
    }

    public void testSerialization_ReasoningExcludeSetsIncludeThoughtsToFalse() throws IOException {
        var request = requestWithReasoning(new Reasoning(ReasoningEffort.HIGH, null, true, null));

        assertJsonEquals(serialize(request, emptyThinkingConfig), """
            {
                "contents": [ { "role": "user", "parts": [ { "text": "Hello, Vertex AI!" } ] } ],
                "generationConfig": {
                    "thinkingConfig": { "thinkingLevel": "HIGH", "includeThoughts": false }
                }
            }
            """);
    }

    public void testSerialization_ReasoningWithoutEffortStillRequestsThoughts() throws IOException {
        // Google has no equivalent of the summary granularity, so a request that only asks for a summary leaves the
        // thinking level to the model and just turns thought summaries on.
        var request = requestWithReasoning(new Reasoning(null, Reasoning.ReasoningSummary.DETAILED, null, true));

        assertJsonEquals(serialize(request, emptyThinkingConfig), """
            {
                "contents": [ { "role": "user", "parts": [ { "text": "Hello, Vertex AI!" } ] } ],
                "generationConfig": {
                    "thinkingConfig": { "includeThoughts": true }
                }
            }
            """);
    }

    public void testSerialization_ThoughtSignatureIsAttachedToMatchingFunctionCall() throws IOException {
        var message = new Message(
            null,
            ASSISTANT_ROLE,
            null,
            List.of(new ToolCall(GOOGLE_TOOL_CALL_ID, new ToolCall.FunctionField(FUNCTION_ARGUMENTS, FUNCTION_NAME), "function")),
            null,
            List.of(new TextReasoningDetail(REASONING_FORMAT, GOOGLE_TOOL_CALL_ID, null, null, THOUGHT_SIGNATURE))
        );

        assertJsonEquals(serialize(requestOf(message), emptyThinkingConfig), Strings.format("""
            {
                "contents": [
                    {
                        "role": "model",
                        "parts": [
                            {
                                "functionCall": { "name": "%s", "args": %s, "id": "%s" },
                                "thoughtSignature": "%s"
                            }
                        ]
                    }
                ]
            }
            """, FUNCTION_NAME, FUNCTION_ARGUMENTS, GOOGLE_TOOL_CALL_ID, THOUGHT_SIGNATURE));
    }

    public void testSerialization_ThoughtSummaryIsWrittenAsThoughtPartBeforeContent() throws IOException {
        var message = new Message(
            new ContentString("The delivery date is March 27."),
            ASSISTANT_ROLE,
            null,
            null,
            null,
            List.of(new TextReasoningDetail(REASONING_FORMAT, null, 0L, "Let me look up the order.", THOUGHT_SIGNATURE))
        );

        assertJsonEquals(serialize(requestOf(message), emptyThinkingConfig), Strings.format("""
            {
                "contents": [
                    {
                        "role": "model",
                        "parts": [
                            { "text": "Let me look up the order.", "thought": true, "thoughtSignature": "%s" },
                            { "text": "The delivery date is March 27." }
                        ]
                    }
                ]
            }
            """, THOUGHT_SIGNATURE));
    }

    public void testSerialization_SignatureWithoutTextOrIdAttachesToLastTextPart() throws IOException {
        var message = new Message(
            new ContentString("The delivery date is March 27."),
            ASSISTANT_ROLE,
            null,
            null,
            null,
            List.of(new TextReasoningDetail(REASONING_FORMAT, null, 0L, null, THOUGHT_SIGNATURE))
        );

        assertJsonEquals(serialize(requestOf(message), emptyThinkingConfig), Strings.format("""
            {
                "contents": [
                    {
                        "role": "model",
                        "parts": [
                            { "text": "The delivery date is March 27.", "thoughtSignature": "%s" }
                        ]
                    }
                ]
            }
            """, THOUGHT_SIGNATURE));
    }

    public void testSerialization_ToolMessageBecomesFunctionResponseWithEchoedId() throws IOException {
        var messages = List.of(assistantToolCall(GOOGLE_TOOL_CALL_ID), toolResult(GOOGLE_TOOL_CALL_ID, TOOL_RESULT_JSON));

        assertJsonEquals(serialize(requestOf(messages), emptyThinkingConfig), Strings.format("""
            {
                "contents": [
                    {
                        "role": "model",
                        "parts": [
                            {
                                "functionCall": { "name": "%s", "args": { "order_id": "order_12345" }, "id": "%s" },
                                "thoughtSignature": "skip_thought_signature_validator"
                            }
                        ]
                    },
                    {
                        "role": "user",
                        "parts": [
                            {
                                "functionResponse": {
                                    "name": "%s",
                                    "id": "%s",
                                    "response": %s
                                }
                            }
                        ]
                    }
                ]
            }
            """, FUNCTION_NAME, GOOGLE_TOOL_CALL_ID, FUNCTION_NAME, GOOGLE_TOOL_CALL_ID, TOOL_RESULT_JSON));
    }

    public void testSerialization_ToolMessageOmitsIdWhenItWasSynthesizedFromTheFunctionName() throws IOException {
        // Responses that carry no function call id fall back to the name, so there is no real id to echo back.
        var messages = List.of(assistantToolCall(FUNCTION_NAME), toolResult(FUNCTION_NAME, TOOL_RESULT_JSON));

        assertJsonEquals(serialize(requestOf(messages), emptyThinkingConfig), Strings.format("""
            {
                "contents": [
                    {
                        "role": "model",
                        "parts": [
                            {
                                "functionCall": { "name": "%s", "args": { "order_id": "order_12345" } },
                                "thoughtSignature": "skip_thought_signature_validator"
                            }
                        ]
                    },
                    {
                        "role": "user",
                        "parts": [
                            {
                                "functionResponse": {
                                    "name": "%s",
                                    "response": %s
                                }
                            }
                        ]
                    }
                ]
            }
            """, FUNCTION_NAME, FUNCTION_NAME, TOOL_RESULT_JSON));
    }

    public void testSerialization_NonJsonToolResultIsWrappedUnderOutput() throws IOException {
        var deliveredOutput = "delivered";
        var messages = List.of(assistantToolCall(GOOGLE_TOOL_CALL_ID), toolResult(GOOGLE_TOOL_CALL_ID, deliveredOutput));

        assertJsonEquals(serialize(requestOf(messages), emptyThinkingConfig), Strings.format("""
            {
                "contents": [
                    {
                        "role": "model",
                        "parts": [
                            {
                                "functionCall": { "name": "%s", "args": { "order_id": "order_12345" }, "id": "%s" },
                                "thoughtSignature": "skip_thought_signature_validator"
                            }
                        ]
                    },
                    {
                        "role": "user",
                        "parts": [
                            {
                                "functionResponse": {
                                    "name": "%s",
                                    "id": "%s",
                                    "response": { "output": "%s" }
                                }
                            }
                        ]
                    }
                ]
            }
            """, FUNCTION_NAME, GOOGLE_TOOL_CALL_ID, FUNCTION_NAME, GOOGLE_TOOL_CALL_ID, deliveredOutput));
    }

    public void testError_ToolMessageWithoutToolCallId() {
        var messages = List.of(new Message(new ContentString(TOOL_RESULT_JSON), TOOL_ROLE, null, null));

        var exception = expectThrows(ElasticsearchStatusException.class, () -> serialize(requestOf(messages), emptyThinkingConfig));
        assertThat(exception.status(), is(RestStatus.BAD_REQUEST));
        assertThat(exception.getMessage(), containsString("Tool messages require a [tool_call_id]"));
    }

    public void testSerialization_EmptyContentObjectDoesNotDropTheRemainingParts() throws IOException {
        List<ContentObject> contentObjects = List.of(
            new ContentObjectText("First part. "),
            new ContentObjectText(""),
            new ContentObjectText("Third part.")
        );
        var message = new Message(new ContentObjects(contentObjects), USER_ROLE, null, null);

        assertJsonEquals(serialize(requestOf(message), emptyThinkingConfig), """
            {
                "contents": [
                    {
                        "role": "user",
                        "parts": [
                            { "text": "First part. " },
                            { "text": "Third part." }
                        ]
                    }
                ]
            }
            """);
    }

    /**
     * The key scenario: a client that does not yet echo {@code reasoning_details} sends an assistant message with tool
     * calls but no signatures. ES should use the sentinel so Gemini 3 does not reject the request with a 400.
     */
    public void testSerialization_FunctionCallWithoutSignature_UsesSkipValidatorSentinel() throws IOException {
        var messages = List.of(assistantToolCall(GOOGLE_TOOL_CALL_ID), toolResult(GOOGLE_TOOL_CALL_ID, TOOL_RESULT_JSON));

        assertJsonEquals(serialize(requestOf(messages), emptyThinkingConfig), Strings.format("""
            {
                "contents": [
                    {
                        "role": "model",
                        "parts": [
                            {
                                "functionCall": { "name": "%s", "args": { "order_id": "order_12345" }, "id": "%s" },
                                "thoughtSignature": "skip_thought_signature_validator"
                            }
                        ]
                    },
                    {
                        "role": "user",
                        "parts": [
                            {
                                "functionResponse": {
                                    "name": "%s",
                                    "id": "%s",
                                    "response": %s
                                }
                            }
                        ]
                    }
                ]
            }
            """, FUNCTION_NAME, GOOGLE_TOOL_CALL_ID, FUNCTION_NAME, GOOGLE_TOOL_CALL_ID, TOOL_RESULT_JSON));
    }

    /**
     * When parallel function calls are present and no signatures are supplied, only the first call gets the sentinel.
     * Google only signs the first function call of a parallel step; subsequent calls are unsigned by design.
     */
    public void testSerialization_ParallelFunctionCallsWithoutSignature_OnlyFirstGetsSentinel() throws IOException {
        var message = parallelAssistantToolCalls();

        assertJsonEquals(serialize(requestOf(message), emptyThinkingConfig), Strings.format("""
            {
                "contents": [
                    {
                        "role": "model",
                        "parts": [
                            {
                                "functionCall": { "name": "%s", "args": { "order_id": "order_12345" }, "id": "%s" },
                                "thoughtSignature": "skip_thought_signature_validator"
                            },
                            {
                                "functionCall": { "name": "%s", "args": {}, "id": "%s" }
                            }
                        ]
                    }
                ]
            }
            """, FUNCTION_NAME, GOOGLE_TOOL_CALL_ID, SECOND_FUNCTION_NAME, SECOND_TOOL_CALL_ID));
    }

    /**
     * When real signatures are present they always take precedence over the sentinel. Both the id-bound path
     * (signature keyed by the tool call id) and the unbound path (a signature with no id and no text) are verified.
     */
    public void testSerialization_RealSignatureTakesPrecedenceOverSentinel_IdBound() throws IOException {
        var message = new Message(
            null,
            ASSISTANT_ROLE,
            null,
            List.of(new ToolCall(GOOGLE_TOOL_CALL_ID, new ToolCall.FunctionField(FUNCTION_ARGUMENTS, FUNCTION_NAME), "function")),
            null,
            List.of(new TextReasoningDetail(REASONING_FORMAT, GOOGLE_TOOL_CALL_ID, null, null, THOUGHT_SIGNATURE))
        );

        assertJsonEquals(serialize(requestOf(message), emptyThinkingConfig), Strings.format("""
            {
                "contents": [
                    {
                        "role": "model",
                        "parts": [
                            {
                                "functionCall": { "name": "%s", "args": %s, "id": "%s" },
                                "thoughtSignature": "%s"
                            }
                        ]
                    }
                ]
            }
            """, FUNCTION_NAME, FUNCTION_ARGUMENTS, GOOGLE_TOOL_CALL_ID, THOUGHT_SIGNATURE));
    }

    public void testSerialization_RealSignatureTakesPrecedenceOverSentinel_Unbound() throws IOException {
        // An unbound signature (no id, no text) lands on the first function call positionally.
        var message = new Message(
            null,
            ASSISTANT_ROLE,
            null,
            List.of(new ToolCall(GOOGLE_TOOL_CALL_ID, new ToolCall.FunctionField(FUNCTION_ARGUMENTS, FUNCTION_NAME), "function")),
            null,
            List.of(new TextReasoningDetail(REASONING_FORMAT, null, 0L, null, THOUGHT_SIGNATURE))
        );

        assertJsonEquals(serialize(requestOf(message), emptyThinkingConfig), Strings.format("""
            {
                "contents": [
                    {
                        "role": "model",
                        "parts": [
                            {
                                "functionCall": { "name": "%s", "args": %s, "id": "%s" },
                                "thoughtSignature": "%s"
                            }
                        ]
                    }
                ]
            }
            """, FUNCTION_NAME, FUNCTION_ARGUMENTS, GOOGLE_TOOL_CALL_ID, THOUGHT_SIGNATURE));
    }

    public void testSerialization_FunctionCallOmitsIdWhenSynthesizedFromFunctionName() throws IOException {
        // When the response carried no id the parser falls back to the function name; there is no real id to echo.
        var message = new Message(
            null,
            ASSISTANT_ROLE,
            null,
            List.of(new ToolCall(FUNCTION_NAME, new ToolCall.FunctionField(FUNCTION_ARGUMENTS, FUNCTION_NAME), "function"))
        );

        assertJsonEquals(serialize(requestOf(message), emptyThinkingConfig), Strings.format("""
            {
                "contents": [
                    {
                        "role": "model",
                        "parts": [
                            {
                                "functionCall": { "name": "%s", "args": %s },
                                "thoughtSignature": "skip_thought_signature_validator"
                            }
                        ]
                    }
                ]
            }
            """, FUNCTION_NAME, FUNCTION_ARGUMENTS));
    }

    /**
     * Parallel tool results (one OpenAI-style {@code tool} message per call) must be merged into a single Gemini
     * {@code user} content with one {@code functionResponse} part per call. This is the exact shape that caused the
     * "number of function response parts" 400 from Vertex AI.
     */
    public void testSerialization_ParallelToolResults_MergedIntoSingleFunctionResponseTurn() throws IOException {
        var secondToolResult = "{\"eta\": \"2h\"}";
        var messages = List.of(
            parallelAssistantToolCalls(),
            toolResult(GOOGLE_TOOL_CALL_ID, TOOL_RESULT_JSON),
            toolResult(SECOND_TOOL_CALL_ID, secondToolResult)
        );

        assertJsonEquals(
            serialize(requestOf(messages), emptyThinkingConfig),
            Strings.format(
                """
                    {
                        "contents": [
                            {
                                "role": "model",
                                "parts": [
                                    {
                                        "functionCall": { "name": "%s", "args": { "order_id": "order_12345" }, "id": "%s" },
                                        "thoughtSignature": "skip_thought_signature_validator"
                                    },
                                    {
                                        "functionCall": { "name": "%s", "args": {}, "id": "%s" }
                                    }
                                ]
                            },
                            {
                                "role": "user",
                                "parts": [
                                    {
                                        "functionResponse": {
                                            "name": "%s",
                                            "id": "%s",
                                            "response": %s
                                        }
                                    },
                                    {
                                        "functionResponse": {
                                            "name": "%s",
                                            "id": "%s",
                                            "response": %s
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                    """,
                FUNCTION_NAME,
                GOOGLE_TOOL_CALL_ID,
                SECOND_FUNCTION_NAME,
                SECOND_TOOL_CALL_ID,
                FUNCTION_NAME,
                GOOGLE_TOOL_CALL_ID,
                TOOL_RESULT_JSON,
                SECOND_FUNCTION_NAME,
                SECOND_TOOL_CALL_ID,
                secondToolResult
            )
        );
    }

    /**
     * Tool results separated by a model turn must not be merged: each pair forms its own alternating turn.
     */
    public void testSerialization_SequentialToolResults_StaySeparateTurns() throws IOException {
        var messages = List.of(
            assistantToolCall(GOOGLE_TOOL_CALL_ID),
            toolResult(GOOGLE_TOOL_CALL_ID, TOOL_RESULT_JSON),
            assistantToolCall(SECOND_TOOL_CALL_ID),
            toolResult(SECOND_TOOL_CALL_ID, TOOL_RESULT_JSON)
        );

        assertJsonEquals(
            serialize(requestOf(messages), emptyThinkingConfig),
            Strings.format(
                """
                    {
                        "contents": [
                            {
                                "role": "model",
                                "parts": [
                                    {
                                        "functionCall": { "name": "%s", "args": { "order_id": "order_12345" }, "id": "%s" },
                                        "thoughtSignature": "skip_thought_signature_validator"
                                    }
                                ]
                            },
                            {
                                "role": "user",
                                "parts": [
                                    {
                                        "functionResponse": {
                                            "name": "%s",
                                            "id": "%s",
                                            "response": %s
                                        }
                                    }
                                ]
                            },
                            {
                                "role": "model",
                                "parts": [
                                    {
                                        "functionCall": { "name": "%s", "args": { "order_id": "order_12345" }, "id": "%s" },
                                        "thoughtSignature": "skip_thought_signature_validator"
                                    }
                                ]
                            },
                            {
                                "role": "user",
                                "parts": [
                                    {
                                        "functionResponse": {
                                            "name": "%s",
                                            "id": "%s",
                                            "response": %s
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                    """,
                FUNCTION_NAME,
                GOOGLE_TOOL_CALL_ID,
                FUNCTION_NAME,
                GOOGLE_TOOL_CALL_ID,
                TOOL_RESULT_JSON,
                FUNCTION_NAME,
                SECOND_TOOL_CALL_ID,
                FUNCTION_NAME,
                SECOND_TOOL_CALL_ID,
                TOOL_RESULT_JSON
            )
        );
    }

    /**
     * A system message between two tool messages must not break the merge — system messages are lifted into
     * {@code systemInstruction} and do not create a content turn of their own.
     */
    public void testSerialization_ToolResultsSeparatedBySystemMessage_AreStillMerged() throws IOException {
        var messages = List.of(
            parallelAssistantToolCalls(),
            toolResult(GOOGLE_TOOL_CALL_ID, TOOL_RESULT_JSON),
            new Message(new ContentObjects(List.of(new ContentObjectText("extra instruction"))), "system", null, null),
            toolResult(SECOND_TOOL_CALL_ID, TOOL_RESULT_JSON)
        );

        assertJsonEquals(
            serialize(requestOf(messages), emptyThinkingConfig),
            Strings.format(
                """
                    {
                        "contents": [
                            {
                                "role": "model",
                                "parts": [
                                    {
                                        "functionCall": { "name": "%s", "args": { "order_id": "order_12345" }, "id": "%s" },
                                        "thoughtSignature": "skip_thought_signature_validator"
                                    },
                                    {
                                        "functionCall": { "name": "%s", "args": {}, "id": "%s" }
                                    }
                                ]
                            },
                            {
                                "role": "user",
                                "parts": [
                                    {
                                        "functionResponse": {
                                            "name": "%s",
                                            "id": "%s",
                                            "response": %s
                                        }
                                    },
                                    {
                                        "functionResponse": {
                                            "name": "%s",
                                            "id": "%s",
                                            "response": %s
                                        }
                                    }
                                ]
                            }
                        ],
                        "systemInstruction": {
                            "parts": [
                                { "text": "extra instruction" }
                            ]
                        }
                    }
                    """,
                FUNCTION_NAME,
                GOOGLE_TOOL_CALL_ID,
                SECOND_FUNCTION_NAME,
                SECOND_TOOL_CALL_ID,
                FUNCTION_NAME,
                GOOGLE_TOOL_CALL_ID,
                TOOL_RESULT_JSON,
                SECOND_FUNCTION_NAME,
                SECOND_TOOL_CALL_ID,
                TOOL_RESULT_JSON
            )
        );
    }

    private static Message assistantToolCall(String toolCallId) {
        return new Message(
            null,
            ASSISTANT_ROLE,
            null,
            List.of(new ToolCall(toolCallId, new ToolCall.FunctionField(FUNCTION_ARGUMENTS, FUNCTION_NAME), "function"))
        );
    }

    private static Message parallelAssistantToolCalls() {
        return new Message(
            null,
            ASSISTANT_ROLE,
            null,
            List.of(
                new ToolCall(GOOGLE_TOOL_CALL_ID, new ToolCall.FunctionField(FUNCTION_ARGUMENTS, FUNCTION_NAME), "function"),
                new ToolCall(SECOND_TOOL_CALL_ID, new ToolCall.FunctionField("{}", SECOND_FUNCTION_NAME), "function")
            )
        );
    }

    private static Message toolResult(String toolCallId, String result) {
        return new Message(new ContentString(result), TOOL_ROLE, toolCallId, null);
    }

    private static UnifiedCompletionRequestBody requestWithReasoning(Reasoning reasoning) {
        return new UnifiedCompletionRequestBody(
            List.of(new Message(new ContentString("Hello, Vertex AI!"), USER_ROLE, null, null)),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            reasoning,
            null,
            null
        );
    }

    private static UnifiedCompletionRequestBody requestOf(Message message) {
        return requestOf(List.of(message));
    }

    private static UnifiedCompletionRequestBody requestOf(List<Message> messages) {
        return new UnifiedCompletionRequestBody(messages, null, null, null, null, null, null, null);
    }

    private static String serialize(UnifiedCompletionRequestBody request, ThinkingConfig config) throws IOException {
        var entity = new GoogleVertexAiUnifiedChatCompletionRequestEntity(new UnifiedChatInput(request, true), config);
        XContentBuilder builder = JsonXContent.contentBuilder();
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);
        return Strings.toString(builder);
    }
}
