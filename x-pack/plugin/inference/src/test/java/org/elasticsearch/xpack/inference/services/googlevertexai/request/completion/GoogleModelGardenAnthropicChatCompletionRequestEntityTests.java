/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.request.completion;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.inference.completion.ContentString;
import org.elasticsearch.inference.completion.Message;
import org.elasticsearch.inference.completion.Tool;
import org.elasticsearch.inference.completion.ToolChoice;
import org.elasticsearch.inference.completion.ToolChoice.ToolChoiceObject;
import org.elasticsearch.inference.completion.ToolChoice.ToolChoiceString;
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

import static org.hamcrest.Matchers.containsString;

public class GoogleModelGardenAnthropicChatCompletionRequestEntityTests extends ESTestCase {

    // OpenAI-shaped tool_choice object {"type":"function","function":{"name":"name"}} must be translated to Anthropic's
    // {"type":"tool","name":"name"} form, otherwise Anthropic rejects the request with a 400 (unexpected tag 'function').
    private static final ToolChoice FUNCTION_TOOL_CHOICE = new ToolChoiceObject("function", new ToolChoiceObject.FunctionField("name"));

    public void testModelUserFieldsSerializationStreamingWithTemperatureAndTopK() throws IOException {
        XContentBuilder builder = setUpXContentBuilder(
            0.2F,
            0.2F,
            100L,
            true,
            FUNCTION_TOOL_CHOICE,
            GoogleVertexAiChatCompletionTaskSettings.EMPTY_SETTINGS
        );
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
                    "type": "tool",
                    "name": "name"
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
        XContentBuilder builder = setUpXContentBuilder(
            null,
            null,
            null,
            false,
            FUNCTION_TOOL_CHOICE,
            GoogleVertexAiChatCompletionTaskSettings.EMPTY_SETTINGS
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
                    "type": "tool",
                    "name": "name"
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
            FUNCTION_TOOL_CHOICE,
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
                    "type": "tool",
                    "name": "name"
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

    public void testToolChoiceStringAutoIsTranslated() throws IOException {
        XContentBuilder builder = setUpXContentBuilder(
            null,
            null,
            null,
            false,
            new ToolChoiceString("auto"),
            GoogleVertexAiChatCompletionTaskSettings.EMPTY_SETTINGS
        );
        assertThat(Strings.toString(builder), containsString("\"tool_choice\":{\"type\":\"auto\"}"));
    }

    public void testToolChoiceStringNoneIsTranslated() throws IOException {
        XContentBuilder builder = setUpXContentBuilder(
            null,
            null,
            null,
            false,
            new ToolChoiceString("none"),
            GoogleVertexAiChatCompletionTaskSettings.EMPTY_SETTINGS
        );
        assertThat(Strings.toString(builder), containsString("\"tool_choice\":{\"type\":\"none\"}"));
    }

    public void testToolChoiceStringRequiredIsTranslatedToAny() throws IOException {
        XContentBuilder builder = setUpXContentBuilder(
            null,
            null,
            null,
            false,
            new ToolChoiceString("required"),
            GoogleVertexAiChatCompletionTaskSettings.EMPTY_SETTINGS
        );
        assertThat(Strings.toString(builder), containsString("\"tool_choice\":{\"type\":\"any\"}"));
    }

    public void testUnsupportedToolChoiceStringThrows() {
        var exception = expectThrows(
            ElasticsearchStatusException.class,
            () -> setUpXContentBuilder(
                null,
                null,
                null,
                false,
                new ToolChoiceString("unsupported"),
                GoogleVertexAiChatCompletionTaskSettings.EMPTY_SETTINGS
            )
        );
        assertThat(exception.getMessage(), containsString("Unsupported tool_choice value [unsupported]"));
    }

    private static XContentBuilder setUpXContentBuilder(
        Float topP,
        Float temperature,
        Long maxCompletionTokens,
        boolean stream,
        ToolChoice toolChoice,
        GoogleVertexAiChatCompletionTaskSettings taskSettings
    ) throws IOException {
        var message = new Message(new ContentString("Hello, world!"), "user", null, null);
        var messageList = new ArrayList<Message>();
        messageList.add(message);
        var unifiedRequest = new UnifiedCompletionRequest(
            messageList,
            null,
            maxCompletionTokens,
            null,
            temperature,
            toolChoice,
            List.of(new Tool("function", new Tool.FunctionField("description", "name", Map.of("parameterName", "parameterValue"), null))),
            topP
        );
        var unifiedChatInput = new UnifiedChatInput(unifiedRequest, stream);
        var entity = new GoogleModelGardenAnthropicChatCompletionRequestEntity(unifiedChatInput.getRequest(), stream, taskSettings);
        var builder = JsonXContent.contentBuilder();
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);
        return builder;
    }
}
