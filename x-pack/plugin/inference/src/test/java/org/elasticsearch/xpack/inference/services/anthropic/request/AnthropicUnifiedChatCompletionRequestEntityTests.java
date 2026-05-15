/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.anthropic.request;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.inference.completion.ContentString;
import org.elasticsearch.inference.completion.Message;
import org.elasticsearch.inference.completion.Tool;
import org.elasticsearch.inference.completion.ToolChoice;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.anthropic.completion.AnthropicChatCompletionTaskSettings;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class AnthropicUnifiedChatCompletionRequestEntityTests extends ESTestCase {

    private static final String MODEL_ID = "claude-3-5-sonnet-latest";
    private static final String ROLE_VALUE = "user";
    private static final String INPUT_VALUE = "Hello, world!";

    public void testSerializationStreamingRequest() throws IOException {
        testSerialization(true, null, null, null, null, taskSettings(1024, null, null, null), Strings.format("""
            {
                "model": "%s",
                "messages": [{"content": "%s", "role": "%s"}],
                "stream": true,
                "max_tokens": 1024
            }
            """, MODEL_ID, INPUT_VALUE, ROLE_VALUE));
    }

    public void testSerializationNonStreamingRequest() throws IOException {
        testSerialization(false, null, null, null, null, taskSettings(512, null, null, null), Strings.format("""
            {
                "model": "%s",
                "messages": [{"content": "%s", "role": "%s"}],
                "stream": false,
                "max_tokens": 512
            }
            """, MODEL_ID, INPUT_VALUE, ROLE_VALUE));
    }

    public void testSerializationWithRequestMaxCompletionTokensOverridesTaskSettings() throws IOException {
        testSerialization(true, 2048L, null, null, null, taskSettings(1024, null, null, null), Strings.format("""
            {
                "model": "%s",
                "messages": [{"content": "%s", "role": "%s"}],
                "stream": true,
                "max_tokens": 2048
            }
            """, MODEL_ID, INPUT_VALUE, ROLE_VALUE));
    }

    public void testSerializationFallsBackToTaskSettingsTemperatureTopPAndTopK() throws IOException {
        testSerialization(true, null, null, null, null, taskSettings(1024, 0.7, 0.9, 50), Strings.format("""
            {
                "model": "%s",
                "messages": [{"content": "%s", "role": "%s"}],
                "temperature": 0.7,
                "top_p": 0.9,
                "top_k": 50,
                "stream": true,
                "max_tokens": 1024
            }
            """, MODEL_ID, INPUT_VALUE, ROLE_VALUE));
    }

    public void testSerializationRequestTemperatureOverridesTaskSettings() throws IOException {
        testSerialization(true, null, 0.2f, 0.3f, null, taskSettings(1024, 0.7, 0.9, null), Strings.format("""
            {
                "model": "%s",
                "messages": [{"content": "%s", "role": "%s"}],
                "temperature": 0.2,
                "top_p": 0.3,
                "stream": true,
                "max_tokens": 1024
            }
            """, MODEL_ID, INPUT_VALUE, ROLE_VALUE));
    }

    public void testSerializationWithToolDefinitionsEmitsAnthropicShape() throws IOException {
        var tool = new Tool("function", new Tool.FunctionField("Get the price of an item", "get_price", Map.of("type", "object"), null));
        testSerialization(true, null, null, null, List.of(tool), taskSettings(1024, null, null, null), Strings.format("""
            {
                "model": "%s",
                "messages": [{"content": "%s", "role": "%s"}],
                "tools": [{"name": "get_price", "description": "Get the price of an item", "input_schema": {"type": "object"}}],
                "stream": true,
                "max_tokens": 1024
            }
            """, MODEL_ID, INPUT_VALUE, ROLE_VALUE));
    }

    public void testSerializationWithToolStrictFieldRejected() {
        var tool = new Tool("function", new Tool.FunctionField("Get the price of an item", "get_price", Map.of("type", "object"), true));
        var entity = entity(true, null, null, null, List.of(tool), null, taskSettings(1024, null, null, null));
        expectThrows(org.elasticsearch.ElasticsearchStatusException.class, () -> {
            try (var builder = JsonXContent.contentBuilder()) {
                entity.toXContent(builder, ToXContent.EMPTY_PARAMS);
            }
        });
    }

    public void testSerializationWithToolChoiceObjectEmitsAnthropicShape() throws IOException {
        // OpenAI ToolChoiceObject uses type "function"; the entity translates it to Anthropic's {"type":"tool","name":"..."}.
        var toolChoice = new ToolChoice.ToolChoiceObject("function", new ToolChoice.ToolChoiceObject.FunctionField("get_price"));
        testSerialization(true, null, null, null, List.of(), toolChoice, taskSettings(1024, null, null, null), Strings.format("""
            {
                "model": "%s",
                "messages": [{"content": "%s", "role": "%s"}],
                "tool_choice": {"type": "tool", "name": "get_price"},
                "stream": true,
                "max_tokens": 1024
            }
            """, MODEL_ID, INPUT_VALUE, ROLE_VALUE));
    }

    public void testSerializationWithToolChoiceStringNoneTranslatesToAnthropicNone() throws IOException {
        var toolChoice = new ToolChoice.ToolChoiceString("none");
        testSerialization(true, null, null, null, List.of(), toolChoice, taskSettings(1024, null, null, null), Strings.format("""
            {
                "model": "%s",
                "messages": [{"content": "%s", "role": "%s"}],
                "tool_choice": {"type": "none"},
                "stream": true,
                "max_tokens": 1024
            }
            """, MODEL_ID, INPUT_VALUE, ROLE_VALUE));
    }

    public void testSerializationWithToolChoiceStringAutoTranslatesToAnthropicAuto() throws IOException {
        var toolChoice = new ToolChoice.ToolChoiceString("auto");
        testSerialization(true, null, null, null, List.of(), toolChoice, taskSettings(1024, null, null, null), Strings.format("""
            {
                "model": "%s",
                "messages": [{"content": "%s", "role": "%s"}],
                "tool_choice": {"type": "auto"},
                "stream": true,
                "max_tokens": 1024
            }
            """, MODEL_ID, INPUT_VALUE, ROLE_VALUE));
    }

    public void testSerializationWithToolChoiceStringRequiredTranslatesToAnthropicAny() throws IOException {
        var toolChoice = new ToolChoice.ToolChoiceString("required");
        testSerialization(true, null, null, null, List.of(), toolChoice, taskSettings(1024, null, null, null), Strings.format("""
            {
                "model": "%s",
                "messages": [{"content": "%s", "role": "%s"}],
                "tool_choice": {"type": "any"},
                "stream": true,
                "max_tokens": 1024
            }
            """, MODEL_ID, INPUT_VALUE, ROLE_VALUE));
    }

    public void testSerializationWithToolChoiceStringUnknownValueRejected() {
        var toolChoice = new ToolChoice.ToolChoiceString("unsupported_value");
        var entity = entity(true, null, null, null, List.of(), toolChoice, taskSettings(1024, null, null, null));
        expectThrows(org.elasticsearch.ElasticsearchStatusException.class, () -> {
            try (var builder = JsonXContent.contentBuilder()) {
                entity.toXContent(builder, ToXContent.EMPTY_PARAMS);
            }
        });
    }

    private static AnthropicChatCompletionTaskSettings taskSettings(int maxTokens, Double temperature, Double topP, Integer topK) {
        return new AnthropicChatCompletionTaskSettings(maxTokens, temperature, topP, topK);
    }

    private static AnthropicUnifiedChatCompletionRequestEntity entity(
        boolean stream,
        Long requestMaxCompletionTokens,
        Float requestTemperature,
        Float requestTopP,
        List<Tool> tools,
        ToolChoice toolChoice,
        AnthropicChatCompletionTaskSettings taskSettings
    ) {
        var message = new Message(new ContentString(INPUT_VALUE), ROLE_VALUE, null, null);
        var unifiedRequest = new UnifiedCompletionRequest(
            List.of(message),
            null,
            requestMaxCompletionTokens,
            null,
            requestTemperature,
            toolChoice,
            tools,
            requestTopP
        );
        var unifiedChatInput = new UnifiedChatInput(unifiedRequest, stream);
        return new AnthropicUnifiedChatCompletionRequestEntity(unifiedChatInput, MODEL_ID, taskSettings);
    }

    private static void testSerialization(
        boolean stream,
        Long requestMaxCompletionTokens,
        Float requestTemperature,
        Float requestTopP,
        List<Tool> tools,
        AnthropicChatCompletionTaskSettings taskSettings,
        String expectedJson
    ) throws IOException {
        testSerialization(stream, requestMaxCompletionTokens, requestTemperature, requestTopP, tools, null, taskSettings, expectedJson);
    }

    private static void testSerialization(
        boolean stream,
        Long requestMaxCompletionTokens,
        Float requestTemperature,
        Float requestTopP,
        List<Tool> tools,
        ToolChoice toolChoice,
        AnthropicChatCompletionTaskSettings taskSettings,
        String expectedJson
    ) throws IOException {
        var entity = entity(stream, requestMaxCompletionTokens, requestTemperature, requestTopP, tools, toolChoice, taskSettings);

        XContentBuilder builder = JsonXContent.contentBuilder();
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertThat(Strings.toString(builder), is(XContentHelper.stripWhitespace(expectedJson)));
    }
}
