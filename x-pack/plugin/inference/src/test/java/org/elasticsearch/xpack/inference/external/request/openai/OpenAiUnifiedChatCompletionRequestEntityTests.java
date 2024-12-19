/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.openai;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.openai.completion.OpenAiChatCompletionModel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Random;

import static org.elasticsearch.xpack.inference.services.openai.completion.OpenAiChatCompletionModelTests.createChatCompletionModel;
import static org.hamcrest.Matchers.equalTo;

public class OpenAiUnifiedChatCompletionRequestEntityTests extends ESTestCase {

    // 1. Basic Serialization
    // Test with minimal required fields to ensure basic serialization works.
    public void testBasicSerialization() throws IOException {
        UnifiedCompletionRequest.Message message = new UnifiedCompletionRequest.Message(
            new UnifiedCompletionRequest.ContentString("Hello, world!"),
            OpenAiUnifiedChatCompletionRequestEntity.USER_FIELD,
            null,
            null,
            null
        );
        var messageList = new ArrayList<UnifiedCompletionRequest.Message>();
        messageList.add(message);
        UnifiedCompletionRequest unifiedRequest = new UnifiedCompletionRequest(messageList, null, null, null, null, null, null, null);

        UnifiedChatInput unifiedChatInput = new UnifiedChatInput(unifiedRequest, true);
        OpenAiChatCompletionModel model = createChatCompletionModel("test-url", "organizationId", "api-key", "test-endpoint", null);

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

    // 2. Serialization with All Fields
    // Test with all possible fields populated to ensure complete serialization.
    public void testSerializationWithAllFields() throws IOException {
        // Create a message with all fields populated
        UnifiedCompletionRequest.Message message = new UnifiedCompletionRequest.Message(
            new UnifiedCompletionRequest.ContentString("Hello, world!"),
            OpenAiUnifiedChatCompletionRequestEntity.USER_FIELD,
            "name",
            "tool_call_id",
            Collections.singletonList(
                new UnifiedCompletionRequest.ToolCall(
                    "id",
                    new UnifiedCompletionRequest.ToolCall.FunctionField("arguments", "function_name"),
                    "type"
                )
            )
        );

        // Create a tool with all fields populated
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
        // Create the unified request with all fields populated
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

        // Create the unified chat input
        UnifiedChatInput unifiedChatInput = new UnifiedChatInput(unifiedRequest, true);

        OpenAiChatCompletionModel model = createChatCompletionModel("test-endpoint", "organizationId", "api-key", "model-name", null);

        // Create the entity
        OpenAiUnifiedChatCompletionRequestEntity entity = new OpenAiUnifiedChatCompletionRequestEntity(unifiedChatInput, model);

        // Serialize to XContent
        XContentBuilder builder = JsonXContent.contentBuilder();
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);

        // Convert to string and verify
        String jsonString = Strings.toString(builder);
        String expectedJson = """
            {
                "messages": [
                    {
                        "content": "Hello, world!",
                        "role": "user",
                        "name": "name",
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

    // 3. Serialization with Null Optional Fields
    // Test with optional fields set to null to ensure they are correctly omitted from the output.
    public void testSerializationWithNullOptionalFields() throws IOException {
        // Create a message with minimal required fields
        UnifiedCompletionRequest.Message message = new UnifiedCompletionRequest.Message(
            new UnifiedCompletionRequest.ContentString("Hello, world!"),
            OpenAiUnifiedChatCompletionRequestEntity.USER_FIELD,
            null,
            null,
            null
        );
        var messageList = new ArrayList<UnifiedCompletionRequest.Message>();
        messageList.add(message);

        // Create the unified request with optional fields set to null
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

        // Create the unified chat input
        UnifiedChatInput unifiedChatInput = new UnifiedChatInput(unifiedRequest, true);

        OpenAiChatCompletionModel model = createChatCompletionModel("test-endpoint", "organizationId", "api-key", "model-name", null);

        // Create the entity
        OpenAiUnifiedChatCompletionRequestEntity entity = new OpenAiUnifiedChatCompletionRequestEntity(unifiedChatInput, model);

        // Serialize to XContent
        XContentBuilder builder = JsonXContent.contentBuilder();
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);

        // Convert to string and verify
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

    // 4. Serialization with Empty Lists
    // Test with fields that are lists set to empty lists to ensure they are correctly serialized.
    public void testSerializationWithEmptyLists() throws IOException {
        // Create a message with minimal required fields
        UnifiedCompletionRequest.Message message = new UnifiedCompletionRequest.Message(
            new UnifiedCompletionRequest.ContentString("Hello, world!"),
            OpenAiUnifiedChatCompletionRequestEntity.USER_FIELD,
            null,
            null,
            Collections.emptyList() // empty toolCalls list
        );
        var messageList = new ArrayList<UnifiedCompletionRequest.Message>();
        messageList.add(message);
        // Create the unified request with empty lists
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

        // Create the unified chat input
        UnifiedChatInput unifiedChatInput = new UnifiedChatInput(unifiedRequest, true);

        OpenAiChatCompletionModel model = createChatCompletionModel("test-endpoint", "organizationId", "api-key", "model-name", null);

        // Create the entity
        OpenAiUnifiedChatCompletionRequestEntity entity = new OpenAiUnifiedChatCompletionRequestEntity(unifiedChatInput, model);

        // Serialize to XContent
        XContentBuilder builder = JsonXContent.contentBuilder();
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);

        // Convert to string and verify
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

    // 5. Serialization with Nested Objects
    // Test with nested objects (e.g., toolCalls, toolChoice, tool) to ensure they are correctly serialized.
    public void testSerializationWithNestedObjects() throws IOException {
        Random random = Randomness.get();

        // Generate random values
        String randomContent = "Hello, world! " + random.nextInt(1000);
        String randomName = "name" + random.nextInt(1000);
        String randomToolCallId = "tool_call_id" + random.nextInt(1000);
        String randomArguments = "arguments" + random.nextInt(1000);
        String randomFunctionName = "function_name" + random.nextInt(1000);
        String randomType = "type" + random.nextInt(1000);
        String randomModel = "model" + random.nextInt(1000);
        String randomStop = "stop" + random.nextInt(1000);
        float randomTemperature = (float) ((float) Math.round(0.5d + (double) random.nextFloat() * 0.5d * 100000d) / 100000d);
        float randomTopP = (float) ((float) Math.round(0.5d + (double) random.nextFloat() * 0.5d * 100000d) / 100000d);

        // Create a message with nested toolCalls
        UnifiedCompletionRequest.Message message = new UnifiedCompletionRequest.Message(
            new UnifiedCompletionRequest.ContentString(randomContent),
            OpenAiUnifiedChatCompletionRequestEntity.USER_FIELD,
            randomName,
            randomToolCallId,
            Collections.singletonList(
                new UnifiedCompletionRequest.ToolCall(
                    "id",
                    new UnifiedCompletionRequest.ToolCall.FunctionField(randomArguments, randomFunctionName),
                    randomType
                )
            )
        );

        // Create a tool with nested function fields
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
        // Create the unified request with nested objects
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

        // Create the unified chat input
        UnifiedChatInput unifiedChatInput = new UnifiedChatInput(unifiedRequest, true);

        OpenAiChatCompletionModel model = createChatCompletionModel("test-endpoint", "organizationId", "api-key", randomModel, null);

        // Create the entity
        OpenAiUnifiedChatCompletionRequestEntity entity = new OpenAiUnifiedChatCompletionRequestEntity(unifiedChatInput, model);

        // Serialize to XContent
        XContentBuilder builder = JsonXContent.contentBuilder();
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);

        // Convert to string and verify
        String jsonString = Strings.toString(builder);
        // Expected JSON should be dynamically generated based on random values
        String expectedJson = String.format(
            Locale.US,
            """
                {
                    "messages": [
                        {
                            "content": "%s",
                            "role": "user",
                            "name": "%s",
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
            randomName,
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

    // 6. Serialization with Different Content Types
    // Test with different content types in messages (e.g., ContentString, ContentObjects) to ensure they are correctly serialized.
    public void testSerializationWithDifferentContentTypes() throws IOException {
        Random random = Randomness.get();

        // Generate random values for ContentString
        String randomContentString = "Hello, world! " + random.nextInt(1000);

        // Generate random values for ContentObjects
        String randomText = "Random text " + random.nextInt(1000);
        String randomType = "type" + random.nextInt(1000);
        UnifiedCompletionRequest.ContentObject contentObject = new UnifiedCompletionRequest.ContentObject(randomText, randomType);

        var contentObjectsList = new ArrayList<UnifiedCompletionRequest.ContentObject>();
        contentObjectsList.add(contentObject);
        UnifiedCompletionRequest.ContentObjects contentObjects = new UnifiedCompletionRequest.ContentObjects(contentObjectsList);

        // Create messages with different content types
        UnifiedCompletionRequest.Message messageWithString = new UnifiedCompletionRequest.Message(
            new UnifiedCompletionRequest.ContentString(randomContentString),
            OpenAiUnifiedChatCompletionRequestEntity.USER_FIELD,
            null,
            null,
            null
        );

        UnifiedCompletionRequest.Message messageWithObjects = new UnifiedCompletionRequest.Message(
            contentObjects,
            OpenAiUnifiedChatCompletionRequestEntity.USER_FIELD,
            null,
            null,
            null
        );
        var messageList = new ArrayList<UnifiedCompletionRequest.Message>();
        messageList.add(messageWithString);
        messageList.add(messageWithObjects);

        // Create the unified request with both types of messages
        UnifiedCompletionRequest unifiedRequest = UnifiedCompletionRequest.of(messageList);

        // Create the unified chat input
        UnifiedChatInput unifiedChatInput = new UnifiedChatInput(unifiedRequest, true);

        OpenAiChatCompletionModel model = createChatCompletionModel("test-endpoint", "organizationId", "api-key", "model-name", null);

        // Create the entity
        OpenAiUnifiedChatCompletionRequestEntity entity = new OpenAiUnifiedChatCompletionRequestEntity(unifiedChatInput, model);

        // Serialize to XContent
        XContentBuilder builder = JsonXContent.contentBuilder();
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);

        // Convert to string and verify
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

    // 7. Serialization with Special Characters
    // Test with special characters in string fields to ensure they are correctly escaped and serialized.
    public void testSerializationWithSpecialCharacters() throws IOException {
        // Create a message with special characters
        UnifiedCompletionRequest.Message message = new UnifiedCompletionRequest.Message(
            new UnifiedCompletionRequest.ContentString("Hello, world! \n \"Special\" characters: \t \\ /"),
            OpenAiUnifiedChatCompletionRequestEntity.USER_FIELD,
            "name\nwith\nnewlines",
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
        // Create the unified request
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

        // Create the unified chat input
        UnifiedChatInput unifiedChatInput = new UnifiedChatInput(unifiedRequest, true);

        OpenAiChatCompletionModel model = createChatCompletionModel("test-endpoint", "organizationId", "api-key", "model-name", null);

        // Create the entity
        OpenAiUnifiedChatCompletionRequestEntity entity = new OpenAiUnifiedChatCompletionRequestEntity(unifiedChatInput, model);

        // Serialize to XContent
        XContentBuilder builder = JsonXContent.contentBuilder();
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);

        // Convert to string and verify
        String jsonString = Strings.toString(builder);
        String expectedJson = """
            {
                "messages": [
                    {
                        "content": "Hello, world! \\n \\"Special\\" characters: \\t \\\\ /",
                        "role": "user",
                        "name": "name\\nwith\\nnewlines",
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

    // 8. Serialization with Boolean Fields
    // Test with boolean fields (stream) set to both true and false to ensure they are correctly serialized.
    public void testSerializationWithBooleanFields() throws IOException {
        // Create a message with minimal required fields
        UnifiedCompletionRequest.Message message = new UnifiedCompletionRequest.Message(
            new UnifiedCompletionRequest.ContentString("Hello, world!"),
            OpenAiUnifiedChatCompletionRequestEntity.USER_FIELD,
            null,
            null,
            null
        );
        var messageList = new ArrayList<UnifiedCompletionRequest.Message>();
        messageList.add(message);
        // Create the unified request
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

        OpenAiChatCompletionModel model = createChatCompletionModel("test-endpoint", "organizationId", "api-key", "model-name", null);

        // Test with stream set to true
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

        // Test with stream set to false
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

    // 9. a test without the content field to show that the content field is optional
    public void testSerializationWithoutContentField() throws IOException {
        UnifiedCompletionRequest.Message message = new UnifiedCompletionRequest.Message(
            null,
            "assistant",
            "name\nwith\nnewlines",
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
        OpenAiChatCompletionModel model = createChatCompletionModel("test-url", "organizationId", "api-key", "test-endpoint", null);

        OpenAiUnifiedChatCompletionRequestEntity entity = new OpenAiUnifiedChatCompletionRequestEntity(unifiedChatInput, model);

        XContentBuilder builder = JsonXContent.contentBuilder();
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String jsonString = Strings.toString(builder);
        String expectedJson = """
            {
                "messages": [
                    {
                        "role": "assistant",
                        "name": "name\\nwith\\nnewlines",
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

    private void assertJsonEquals(String actual, String expected) throws IOException {
        try (
            var actualParser = createParser(JsonXContent.jsonXContent, actual);
            var expectedParser = createParser(JsonXContent.jsonXContent, expected)
        ) {
            assertThat(actualParser.mapOrdered(), equalTo(expectedParser.mapOrdered()));
        }
    }

}
