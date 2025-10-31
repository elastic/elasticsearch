/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.request.completion;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.googlevertexai.completion.GoogleVertexAiChatCompletionTaskSettings;
import org.elasticsearch.xpack.inference.services.googlevertexai.completion.ThinkingConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class GoogleModelGardenAnthropicChatCompletionRequestEntityTests extends ESTestCase {

    public void testModelUserFieldsSerializationStreamingWithTemperatureAndTopK() throws IOException {
        XContentBuilder builder = setUpXContentBuilder(0.2F, 0.2F, 100L, true, GoogleVertexAiChatCompletionTaskSettings.EMPTY_SETTINGS);
        String expectedJson = """
            {
                "anthropic_version": "vertex-2023-10-16",
                "messages": [{
                        "content": "Hello, world!",
                        "role": "user"
                    }
                ],
                "temperature": 0.2,
                "tool_choice": {
                    "type": "auto"
                },
                "tools": [{
                        "name": "name",
                        "description": "description",
                        "input_schema": {
                            "parameterName": "parameterValue"
                        }
                    }
                ],
                "top_p": 0.2,
                "stream": true,
                "max_tokens": 100
            }
            """;
        assertEquals(XContentHelper.stripWhitespace(expectedJson), Strings.toString(builder));
    }

    public void testModelUserFieldsSerializationNonStreamDefaultMaxTokens() throws IOException {
        XContentBuilder builder = setUpXContentBuilder(null, null, null, false, GoogleVertexAiChatCompletionTaskSettings.EMPTY_SETTINGS);
        String expectedJson = """
            {
                "anthropic_version": "vertex-2023-10-16",
                "messages": [{
                        "content": "Hello, world!",
                        "role": "user"
                    }
                ],
                "tool_choice": {
                    "type": "auto"
                },
                "tools": [{
                        "name": "name",
                        "description": "description",
                        "input_schema": {
                            "parameterName": "parameterValue"
                        }
                    }
                ],
                "stream": false,
                "max_tokens": 1024
            }
            """;
        assertEquals(XContentHelper.stripWhitespace(expectedJson), Strings.toString(builder));
    }

    public void testModelUserFieldsSerializationNonStreamWithMaxTokensFromTaskSettings() throws IOException {
        XContentBuilder builder = setUpXContentBuilder(
            null,
            null,
            null,
            false,
            new GoogleVertexAiChatCompletionTaskSettings(new ThinkingConfig(123), 123)
        );
        String expectedJson = """
            {
                "anthropic_version": "vertex-2023-10-16",
                "messages": [{
                        "content": "Hello, world!",
                        "role": "user"
                    }
                ],
                "tool_choice": {
                    "type": "auto"
                },
                "tools": [{
                        "name": "name",
                        "description": "description",
                        "input_schema": {
                            "parameterName": "parameterValue"
                        }
                    }
                ],
                "stream": false,
                "max_tokens": 123
            }
            """;
        assertEquals(XContentHelper.stripWhitespace(expectedJson), Strings.toString(builder));
    }

    private static XContentBuilder setUpXContentBuilder(
        Float topP,
        Float temperature,
        Long maxCompletionTokens,
        boolean stream,
        GoogleVertexAiChatCompletionTaskSettings taskSettings
    ) throws IOException {
        var message = new UnifiedCompletionRequest.Message(new UnifiedCompletionRequest.ContentString("Hello, world!"), "user", null, null);
        var messageList = new ArrayList<UnifiedCompletionRequest.Message>();
        messageList.add(message);
        var unifiedRequest = new UnifiedCompletionRequest(
            messageList,
            null,
            maxCompletionTokens,
            null,
            temperature,
            new UnifiedCompletionRequest.ToolChoiceObject("auto", new UnifiedCompletionRequest.ToolChoiceObject.FunctionField("name")),
            List.of(
                new UnifiedCompletionRequest.Tool(
                    "function",
                    new UnifiedCompletionRequest.Tool.FunctionField("description", "name", Map.of("parameterName", "parameterValue"), null)
                )
            ),
            topP
        );
        var unifiedChatInput = new UnifiedChatInput(unifiedRequest, stream);
        var entity = new GoogleModelGardenAnthropicChatCompletionRequestEntity(unifiedChatInput.getRequest(), stream, taskSettings);
        var builder = JsonXContent.contentBuilder();
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);
        return builder;
    }
}
