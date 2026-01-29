/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.unified;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.openai.completion.OpenAiChatCompletionModel;
import org.elasticsearch.xpack.inference.services.openai.request.OpenAiUnifiedChatCompletionRequestEntity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Random;

import static org.elasticsearch.xpack.inference.Utils.assertJsonEquals;
import static org.elasticsearch.xpack.inference.services.openai.completion.OpenAiChatCompletionModelTests.createCompletionModel;

public class UnifiedChatCompletionRequestEntityTests extends ESTestCase {

    private static final String ROLE = "user";

    public void testBasicSerialization() throws IOException {
        UnifiedCompletionRequest.Message message = new UnifiedCompletionRequest.Message(
            new UnifiedCompletionRequest.ContentString("Hello, world!"),
            ROLE,
            null,
            null
        );
        var messageList = new ArrayList<UnifiedCompletionRequest.Message>();
        messageList.add(message);
        UnifiedCompletionRequest unifiedRequest = new UnifiedCompletionRequest(messageList, null, null, null, null, null, null, null);

        UnifiedChatInput unifiedChatInput = new UnifiedChatInput(unifiedRequest, true);
        OpenAiChatCompletionModel model = createCompletionModel("test-url", "organizationId", "api-key", "test-endpoint", null);

        OpenAiUnifiedChatCompletionRequestEntity entity = new OpenAiUnifiedChatCompletionRequestEntity(unifiedChatInput, model);

        XContentBuilder builder = JsonXContent.contentBuilder();
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String jsonString = Strings.toString(builder);
        String expectedJson = """
            {
                "messages": [
                    {
                        "content": "Hello, world!",
                        "role": "user"
                    }
                ],
                "model": "test-endpoint",
                "n": 1,
                "stream": true,
                "stream_options": {
                    "include_usage": true
                }
            }
            """;
        assertJsonEquals(jsonString, expectedJson);
    }

    public void testSerializationWithAllFields() throws IOException {
        UnifiedCompletionRequest.Message message = new UnifiedCompletionRequest.Message(
            new UnifiedCompletionRequest.ContentString("Hello, world!"),
            ROLE,
            "tool_call_id",
            Collections.singletonList(
                new UnifiedCompletionRequest.ToolCall(
                    "id",
                    new UnifiedCompletionRequest.ToolCall.FunctionField("arguments", "function_name"),
                    "type"
                )
            )
        );

        UnifiedCompletionRequest.Tool tool = new UnifiedCompletionRequest.Tool(
            "type",
            new UnifiedCompletionRequest.Tool.FunctionField(
                "Fetches the weather in the given location",
                "get_weather",
                createParameters(),
                true
            )
        );
        var messageList = new ArrayList<UnifiedCompletionRequest.Message>();
        messageList.add(message);
        UnifiedCompletionRequest unifiedRequest = new UnifiedCompletionRequest(
            messageList,
            "model",
            100L, // maxCompletionTokens
            Collections.singletonList("stop"),
            0.9f, // temperature
            new UnifiedCompletionRequest.ToolChoiceString("tool_choice"),
            Collections.singletonList(tool),
            0.8f // topP
        );

        UnifiedChatInput unifiedChatInput = new UnifiedChatInput(unifiedRequest, true);

        OpenAiChatCompletionModel model = createCompletionModel("test-endpoint", "organizationId", "api-key", "model-name", null);

        OpenAiUnifiedChatCompletionRequestEntity entity = new OpenAiUnifiedChatCompletionRequestEntity(unifiedChatInput, model);

        XContentBuilder builder = JsonXContent.contentBuilder();
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String jsonString = Strings.toString(builder);
        String expectedJson = """
            {
                "messages": [
                    {
                        "content": "Hello, world!",
                        "role": "user",
                        "tool_call_id": "tool_call_id",
                        "tool_calls": [
                            {
                                "id": "id",
                                "function": {
                                    "arguments": "arguments",
                                    "name": "function_name"
                                },
                                "type": "type"
                            }
                        ]
                    }
                ],
                "model": "model-name",
                "max_completion_tokens": 100,
                "n": 1,
                "stop": ["stop"],
                "temperature": 0.9,
                "tool_choice": "tool_choice",
                "tools": [
                    {
                        "type": "type",
                        "function": {
                            "description": "Fetches the weather in the given location",
                            "name": "get_weather",
                            "parameters": {
                                "type": "object",
                                "properties": {
                                    "location": {
                                        "description": "The location to get the weather for",
                                        "type": "string"
                                    },
                                    "unit": {
                                        "description": "The unit to return the temperature in",
                                        "type": "string",
                                        "enum": ["F", "C"]
                                    }
                                },
                                "additionalProperties": false,
                                "required": ["location", "unit"]
                            },
                            "strict": true
                        }
                    }
                ],
                "top_p": 0.8,
                "stream": true,
                "stream_options": {
                    "include_usage": true
                }
            }
            """;
        assertJsonEquals(jsonString, expectedJson);

    }

    public void testSerializationWithNullOptionalFields() throws IOException {
        UnifiedCompletionRequest.Message message = new UnifiedCompletionRequest.Message(
            new UnifiedCompletionRequest.ContentString("Hello, world!"),
            ROLE,
            null,
            null
        );
        var messageList = new ArrayList<UnifiedCompletionRequest.Message>();
        messageList.add(message);

        UnifiedCompletionRequest unifiedRequest = new UnifiedCompletionRequest(
            messageList,
            null, // model
            null, // maxCompletionTokens
            null, // stop
            null, // temperature
            null, // toolChoice
            null, // tools
            null  // topP
        );

        UnifiedChatInput unifiedChatInput = new UnifiedChatInput(unifiedRequest, true);

        OpenAiChatCompletionModel model = createCompletionModel("test-endpoint", "organizationId", "api-key", "model-name", null);

        OpenAiUnifiedChatCompletionRequestEntity entity = new OpenAiUnifiedChatCompletionRequestEntity(unifiedChatInput, model);

        XContentBuilder builder = JsonXContent.contentBuilder();
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String jsonString = Strings.toString(builder);
        String expectedJson = """
            {
                "messages": [
                    {
                        "content": "Hello, world!",
                        "role": "user"
                    }
                ],
                "model": "model-name",
                "n": 1,
                "stream": true,
                "stream_options": {
                    "include_usage": true
                }
            }
            """;
        assertJsonEquals(jsonString, expectedJson);
    }

    public void testSerializationWithEmptyLists() throws IOException {
        UnifiedCompletionRequest.Message message = new UnifiedCompletionRequest.Message(
            new UnifiedCompletionRequest.ContentString("Hello, world!"),
            ROLE,
            null,
            Collections.emptyList() // empty toolCalls list
        );
        var messageList = new ArrayList<UnifiedCompletionRequest.Message>();
        messageList.add(message);
        UnifiedCompletionRequest unifiedRequest = new UnifiedCompletionRequest(
            messageList,
            null, // model
            null, // maxCompletionTokens
            Collections.emptyList(), // empty stop list
            null, // temperature
            null, // toolChoice
            Collections.emptyList(), // empty tools list
            null  // topP
        );

        UnifiedChatInput unifiedChatInput = new UnifiedChatInput(unifiedRequest, true);

        OpenAiChatCompletionModel model = createCompletionModel("test-endpoint", "organizationId", "api-key", "model-name", null);

        OpenAiUnifiedChatCompletionRequestEntity entity = new OpenAiUnifiedChatCompletionRequestEntity(unifiedChatInput, model);

        XContentBuilder builder = JsonXContent.contentBuilder();
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String jsonString = Strings.toString(builder);
        String expectedJson = """
            {
                "messages": [
                    {
                        "content": "Hello, world!",
                        "role": "user",
                        "tool_calls": []
                    }
                ],
                "model": "model-name",
                "n": 1,
                "stream": true,
                "stream_options": {
                    "include_usage": true
                }
            }
            """;
        assertJsonEquals(jsonString, expectedJson);
    }

    public void testSerializationWithNestedObjects() throws IOException {
        Random random = Randomness.get();

        String randomContent = "Hello, world! " + random.nextInt(1000);
        String randomToolCallId = "tool_call_id" + random.nextInt(1000);
        String randomArguments = "arguments" + random.nextInt(1000);
        String randomFunctionName = "function_name" + random.nextInt(1000);
        String randomType = "type" + random.nextInt(1000);
        String randomModel = "model" + random.nextInt(1000);
        String randomStop = "stop" + random.nextInt(1000);
        float randomTemperature = (float) ((float) Math.round(0.5d + (double) random.nextFloat() * 0.5d * 100000d) / 100000d);
        float randomTopP = (float) ((float) Math.round(0.5d + (double) random.nextFloat() * 0.5d * 100000d) / 100000d);

        UnifiedCompletionRequest.Message message = new UnifiedCompletionRequest.Message(
            new UnifiedCompletionRequest.ContentString(randomContent),
            ROLE,
            randomToolCallId,
            Collections.singletonList(
                new UnifiedCompletionRequest.ToolCall(
                    "id",
                    new UnifiedCompletionRequest.ToolCall.FunctionField(randomArguments, randomFunctionName),
                    randomType
                )
            )
        );

        UnifiedCompletionRequest.Tool tool = new UnifiedCompletionRequest.Tool(
            randomType,
            new UnifiedCompletionRequest.Tool.FunctionField(
                "Fetches the weather in the given location",
                "get_weather",
                createParameters(),
                true
            )
        );
        var messageList = new ArrayList<UnifiedCompletionRequest.Message>();
        messageList.add(message);
        UnifiedCompletionRequest unifiedRequest = new UnifiedCompletionRequest(
            messageList,
            randomModel,
            100L, // maxCompletionTokens
            Collections.singletonList(randomStop),
            randomTemperature, // temperature
            new UnifiedCompletionRequest.ToolChoiceObject(
                randomType,
                new UnifiedCompletionRequest.ToolChoiceObject.FunctionField(randomFunctionName)
            ),
            Collections.singletonList(tool),
            randomTopP // topP
        );

        UnifiedChatInput unifiedChatInput = new UnifiedChatInput(unifiedRequest, true);

        OpenAiChatCompletionModel model = createCompletionModel("test-endpoint", "organizationId", "api-key", randomModel, null);

        OpenAiUnifiedChatCompletionRequestEntity entity = new OpenAiUnifiedChatCompletionRequestEntity(unifiedChatInput, model);

        XContentBuilder builder = JsonXContent.contentBuilder();
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String jsonString = Strings.toString(builder);
        String expectedJson = String.format(
            Locale.US,
            """
                {
                    "messages": [
                        {
                            "content": "%s",
                            "role": "user",
                            "tool_call_id": "%s",
                            "tool_calls": [
                                {
                                    "id": "id",
                                    "function": {
                                        "arguments": "%s",
                                        "name": "%s"
                                    },
                                    "type": "%s"
                                }
                            ]
                        }
                    ],
                    "model": "%s",
                    "max_completion_tokens": 100,
                    "n": 1,
                    "stop": ["%s"],
                    "temperature": %.5f,
                    "tool_choice": {
                        "type": "%s",
                        "function": {
                            "name": "%s"
                        }
                    },
                    "tools": [
                        {
                            "type": "%s",
                            "function": {
                                "description": "Fetches the weather in the given location",
                                "name": "get_weather",
                                "parameters": {
                                    "type": "object",
                                    "properties": {
                                        "unit": {
                                            "description": "The unit to return the temperature in",
                                            "type": "string",
                                            "enum": ["F", "C"]
                                        },
                                        "location": {
                                            "description": "The location to get the weather for",
                                            "type": "string"
                                        }
                                    },
                                    "additionalProperties": false,
                                    "required": ["location", "unit"]
                                },
                                "strict": true
                            }
                        }
                    ],
                    "top_p": %.5f,
                    "stream": true,
                    "stream_options": {
                        "include_usage": true
                    }
                }
                """,
            randomContent,
            randomToolCallId,
            randomArguments,
            randomFunctionName,
            randomType,
            randomModel,
            randomStop,
            randomTemperature,
            randomType,
            randomFunctionName,
            randomType,
            randomTopP
        );
        assertJsonEquals(jsonString, expectedJson);
    }

    public void testSerializationWithDifferentContentTypes() throws IOException {
        Random random = Randomness.get();

        String randomContentString = "Hello, world! " + random.nextInt(1000);

        String randomText = "Random text " + random.nextInt(1000);
        String randomType = "type" + random.nextInt(1000);
        UnifiedCompletionRequest.ContentObject contentObject = new UnifiedCompletionRequest.ContentObject(randomText, randomType);

        var contentObjectsList = new ArrayList<UnifiedCompletionRequest.ContentObject>();
        contentObjectsList.add(contentObject);
        UnifiedCompletionRequest.ContentObjects contentObjects = new UnifiedCompletionRequest.ContentObjects(contentObjectsList);

        UnifiedCompletionRequest.Message messageWithString = new UnifiedCompletionRequest.Message(
            new UnifiedCompletionRequest.ContentString(randomContentString),
            ROLE,
            null,
            null
        );

        UnifiedCompletionRequest.Message messageWithObjects = new UnifiedCompletionRequest.Message(contentObjects, ROLE, null, null);
        var messageList = new ArrayList<UnifiedCompletionRequest.Message>();
        messageList.add(messageWithString);
        messageList.add(messageWithObjects);

        UnifiedCompletionRequest unifiedRequest = UnifiedCompletionRequest.of(messageList);

        UnifiedChatInput unifiedChatInput = new UnifiedChatInput(unifiedRequest, true);

        OpenAiChatCompletionModel model = createCompletionModel("test-endpoint", "organizationId", "api-key", "model-name", null);

        OpenAiUnifiedChatCompletionRequestEntity entity = new OpenAiUnifiedChatCompletionRequestEntity(unifiedChatInput, model);

        XContentBuilder builder = JsonXContent.contentBuilder();
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String jsonString = Strings.toString(builder);
        String expectedJson = String.format(Locale.US, """
            {
                "messages": [
                    {
                        "content": "%s",
                        "role": "user"
                    },
                    {
                        "content": [
                            {
                                "text": "%s",
                                "type": "%s"
                            }
                        ],
                        "role": "user"
                    }
                ],
                "model": "model-name",
                "n": 1,
                "stream": true,
                "stream_options": {
                    "include_usage": true
                }
            }
            """, randomContentString, randomText, randomType);
        assertJsonEquals(jsonString, expectedJson);
    }

    public void testSerializationWithSpecialCharacters() throws IOException {
        UnifiedCompletionRequest.Message message = new UnifiedCompletionRequest.Message(
            new UnifiedCompletionRequest.ContentString("Hello, world! \n \"Special\" characters: \t \\ /"),
            ROLE,
            "tool_call_id\twith\ttabs",
            Collections.singletonList(
                new UnifiedCompletionRequest.ToolCall(
                    "id\\with\\backslashes",
                    new UnifiedCompletionRequest.ToolCall.FunctionField("arguments\"with\"quotes", "function_name/with/slashes"),
                    "type"
                )
            )
        );
        var messageList = new ArrayList<UnifiedCompletionRequest.Message>();
        messageList.add(message);
        UnifiedCompletionRequest unifiedRequest = new UnifiedCompletionRequest(
            messageList,
            null, // model
            null, // maxCompletionTokens
            null, // stop
            null, // temperature
            null, // toolChoice
            null, // tools
            null  // topP
        );

        UnifiedChatInput unifiedChatInput = new UnifiedChatInput(unifiedRequest, true);

        OpenAiChatCompletionModel model = createCompletionModel("test-endpoint", "organizationId", "api-key", "model-name", null);

        OpenAiUnifiedChatCompletionRequestEntity entity = new OpenAiUnifiedChatCompletionRequestEntity(unifiedChatInput, model);

        XContentBuilder builder = JsonXContent.contentBuilder();
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String jsonString = Strings.toString(builder);
        String expectedJson = """
            {
                "messages": [
                    {
                        "content": "Hello, world! \\n \\"Special\\" characters: \\t \\\\ /",
                        "role": "user",
                        "tool_call_id": "tool_call_id\\twith\\ttabs",
                        "tool_calls": [
                            {
                                "id": "id\\\\with\\\\backslashes",
                                "function": {
                                    "arguments": "arguments\\"with\\"quotes",
                                    "name": "function_name/with/slashes"
                                },
                                "type": "type"
                            }
                        ]
                    }
                ],
                "model": "model-name",
                "n": 1,
                "stream": true,
                "stream_options": {
                    "include_usage": true
                }
            }
            """;
        assertJsonEquals(jsonString, expectedJson);
    }

    public void testSerializationWithBooleanFields() throws IOException {
        UnifiedCompletionRequest.Message message = new UnifiedCompletionRequest.Message(
            new UnifiedCompletionRequest.ContentString("Hello, world!"),
            ROLE,
            null,
            null
        );
        var messageList = new ArrayList<UnifiedCompletionRequest.Message>();
        messageList.add(message);
        UnifiedCompletionRequest unifiedRequest = new UnifiedCompletionRequest(
            messageList,
            null, // model
            null, // maxCompletionTokens
            null, // stop
            null, // temperature
            null, // toolChoice
            null, // tools
            null  // topP
        );

        OpenAiChatCompletionModel model = createCompletionModel("test-endpoint", "organizationId", "api-key", "model-name", null);

        UnifiedChatInput unifiedChatInputTrue = new UnifiedChatInput(unifiedRequest, true);
        OpenAiUnifiedChatCompletionRequestEntity entityTrue = new OpenAiUnifiedChatCompletionRequestEntity(unifiedChatInputTrue, model);

        XContentBuilder builderTrue = JsonXContent.contentBuilder();
        entityTrue.toXContent(builderTrue, ToXContent.EMPTY_PARAMS);

        String jsonStringTrue = Strings.toString(builderTrue);
        String expectedJsonTrue = """
            {
                "messages": [
                    {
                        "content": "Hello, world!",
                        "role": "user"
                    }
                ],
                "model": "model-name",
                "n": 1,
                "stream": true,
                "stream_options": {
                    "include_usage": true
                }
            }
            """;
        assertJsonEquals(expectedJsonTrue, jsonStringTrue);

        UnifiedChatInput unifiedChatInputFalse = new UnifiedChatInput(unifiedRequest, false);
        OpenAiUnifiedChatCompletionRequestEntity entityFalse = new OpenAiUnifiedChatCompletionRequestEntity(unifiedChatInputFalse, model);

        XContentBuilder builderFalse = JsonXContent.contentBuilder();
        entityFalse.toXContent(builderFalse, ToXContent.EMPTY_PARAMS);

        String jsonStringFalse = Strings.toString(builderFalse);
        String expectedJsonFalse = """
            {
                "messages": [
                    {
                        "content": "Hello, world!",
                        "role": "user"
                    }
                ],
                "model": "model-name",
                "n": 1,
                "stream": false
            }
            """;
        assertJsonEquals(expectedJsonFalse, jsonStringFalse);
    }

    public void testSerializationWithoutContentField() throws IOException {
        UnifiedCompletionRequest.Message message = new UnifiedCompletionRequest.Message(
            null,
            "assistant",
            "tool_call_id\twith\ttabs",
            Collections.singletonList(
                new UnifiedCompletionRequest.ToolCall(
                    "id\\with\\backslashes",
                    new UnifiedCompletionRequest.ToolCall.FunctionField("arguments\"with\"quotes", "function_name/with/slashes"),
                    "type"
                )
            )
        );
        var messageList = new ArrayList<UnifiedCompletionRequest.Message>();
        messageList.add(message);
        UnifiedCompletionRequest unifiedRequest = new UnifiedCompletionRequest(messageList, null, null, null, null, null, null, null);

        UnifiedChatInput unifiedChatInput = new UnifiedChatInput(unifiedRequest, true);
        OpenAiChatCompletionModel model = createCompletionModel("test-url", "organizationId", "api-key", "test-endpoint", null);

        OpenAiUnifiedChatCompletionRequestEntity entity = new OpenAiUnifiedChatCompletionRequestEntity(unifiedChatInput, model);

        XContentBuilder builder = JsonXContent.contentBuilder();
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String jsonString = Strings.toString(builder);
        String expectedJson = """
            {
                "messages": [
                    {
                        "role": "assistant",
                        "tool_call_id": "tool_call_id\\twith\\ttabs",
                        "tool_calls": [
                            {
                                "id": "id\\\\with\\\\backslashes",
                                "function": {
                                    "arguments": "arguments\\"with\\"quotes",
                                    "name": "function_name/with/slashes"
                                },
                                "type": "type"
                            }
                        ]
                   }
                ],
                "model": "test-endpoint",
                "n": 1,
                "stream": true,
                "stream_options": {
                    "include_usage": true
                }
            }
            """;
        assertJsonEquals(jsonString, expectedJson);
    }

    public static Map<String, Object> createParameters() {
        Map<String, Object> parameters = new LinkedHashMap<>();
        parameters.put("type", "object");

        Map<String, Object> properties = new HashMap<>();

        Map<String, Object> location = new HashMap<>();
        location.put("type", "string");
        location.put("description", "The location to get the weather for");
        properties.put("location", location);

        Map<String, Object> unit = new HashMap<>();
        unit.put("type", "string");
        unit.put("description", "The unit to return the temperature in");
        unit.put("enum", new String[] { "F", "C" });
        properties.put("unit", unit);

        parameters.put("properties", properties);
        parameters.put("additionalProperties", false);
        parameters.put("required", new String[] { "location", "unit" });

        return parameters;
    }
}
