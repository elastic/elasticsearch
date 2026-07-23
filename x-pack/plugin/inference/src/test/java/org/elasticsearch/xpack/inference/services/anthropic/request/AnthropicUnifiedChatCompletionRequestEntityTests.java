/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.anthropic.request;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.inference.completion.ContentString;
import org.elasticsearch.inference.completion.Message;
import org.elasticsearch.inference.completion.Tool;
import org.elasticsearch.inference.completion.ToolChoice;
import org.elasticsearch.rest.RestStatus;
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

    public void testToXContent_StreamingRequest() throws IOException {
        var maxTokens = 1024;
        testToXContent(true, null, null, null, null, null, taskSettings(maxTokens, null, null, null), Strings.format("""
            {
                "model": "%s",
                "messages": [{"content": "%s", "role": "%s"}],
                "stream": true,
                "max_tokens": %d
            }
            """, MODEL_ID, INPUT_VALUE, ROLE_VALUE, maxTokens));
    }

    public void testToXContent_NonStreamingRequest() throws IOException {
        var maxTokens = 512;
        testToXContent(false, null, null, null, null, null, taskSettings(maxTokens, null, null, null), Strings.format("""
            {
                "model": "%s",
                "messages": [{"content": "%s", "role": "%s"}],
                "stream": false,
                "max_tokens": %d
            }
            """, MODEL_ID, INPUT_VALUE, ROLE_VALUE, maxTokens));
    }

    public void testToXContent_RequestOverridesTaskSettings() throws IOException {
        var taskMaxTokens = 1024;
        var requestMaxTokens = 2048L;
        var requestTemperature = 0.2f;
        var requestTopP = 0.3f;
        testToXContent(
            true,
            requestMaxTokens,
            requestTemperature,
            requestTopP,
            null,
            null,
            taskSettings(taskMaxTokens, 0.7, 0.9, null),
            Strings.format("""
                {
                    "model": "%s",
                    "messages": [{"content": "%s", "role": "%s"}],
                    "temperature": %s,
                    "top_p": %s,
                    "stream": true,
                    "max_tokens": %d
                }
                """, MODEL_ID, INPUT_VALUE, ROLE_VALUE, requestTemperature, requestTopP, requestMaxTokens)
        );
    }

    public void testToXContent_FallsBackToTaskSettingsTemperatureTopPAndTopK() throws IOException {
        var maxTokens = 1024;
        testToXContent(true, null, null, null, null, null, taskSettings(maxTokens, 0.7, 0.9, 50), Strings.format("""
            {
                "model": "%s",
                "messages": [{"content": "%s", "role": "%s"}],
                "temperature": 0.7,
                "top_p": 0.9,
                "top_k": 50,
                "stream": true,
                "max_tokens": %d
            }
            """, MODEL_ID, INPUT_VALUE, ROLE_VALUE, maxTokens));
    }

    public void testToXContent_WithToolDefinitionsEmitsAnthropicShape() throws IOException {
        var maxTokens = 1024;
        var tool = new Tool("function", new Tool.FunctionField("Get the price of an item", "get_price", Map.of("type", "object"), null));
        testToXContent(true, null, null, null, List.of(tool), null, taskSettings(maxTokens, null, null, null), Strings.format("""
            {
                "model": "%s",
                "messages": [{"content": "%s", "role": "%s"}],
                "tools": [
                    {
                        "name": "get_price",
                        "description": "Get the price of an item",
                        "input_schema": {"type": "object", "properties": {}}
                    }
                ],
                "stream": true,
                "max_tokens": %d
            }
            """, MODEL_ID, INPUT_VALUE, ROLE_VALUE, maxTokens));
    }

    public void testToXContent_WithToolStrictFieldIgnored() throws IOException {
        // The OpenAI "strict" field has no Anthropic equivalent, so it is silently dropped rather than rejected.
        var maxTokens = 1024;
        var tool = new Tool(
            "function",
            new Tool.FunctionField("Get the price of an item", "get_price", Map.of("type", "object"), randomBoolean())
        );
        testToXContent(true, null, null, null, List.of(tool), null, taskSettings(maxTokens, null, null, null), Strings.format("""
            {
                "model": "%s",
                "messages": [{"content": "%s", "role": "%s"}],
                "tools": [
                    {
                        "name": "get_price",
                        "description": "Get the price of an item",
                        "input_schema": {"type": "object", "properties": {}}
                    }
                ],
                "stream": true,
                "max_tokens": %d
            }
            """, MODEL_ID, INPUT_VALUE, ROLE_VALUE, maxTokens));
    }

    public void testToXContent_WithToolChoiceObjectEmitsAnthropicShape() throws IOException {
        var maxTokens = 1024;
        var toolChoice = new ToolChoice.ToolChoiceObject("function", new ToolChoice.ToolChoiceObject.FunctionField("get_price"));
        testToXContent(true, null, null, null, List.of(), toolChoice, taskSettings(maxTokens, null, null, null), Strings.format("""
            {
                "model": "%s",
                "messages": [{"content": "%s", "role": "%s"}],
                "tool_choice": {"type": "tool", "name": "get_price"},
                "stream": true,
                "max_tokens": %d
            }
            """, MODEL_ID, INPUT_VALUE, ROLE_VALUE, maxTokens));
    }

    public void testToXContent_WithToolChoiceStringNoneTranslatesToAnthropicNone() throws IOException {
        var maxTokens = 1024;
        var toolChoice = new ToolChoice.ToolChoiceString("none");
        testToXContent(true, null, null, null, List.of(), toolChoice, taskSettings(maxTokens, null, null, null), Strings.format("""
            {
                "model": "%s",
                "messages": [{"content": "%s", "role": "%s"}],
                "tool_choice": {"type": "none"},
                "stream": true,
                "max_tokens": %d
            }
            """, MODEL_ID, INPUT_VALUE, ROLE_VALUE, maxTokens));
    }

    public void testToXContent_WithToolChoiceStringAutoTranslatesToAnthropicAuto() throws IOException {
        var maxTokens = 1024;
        var toolChoice = new ToolChoice.ToolChoiceString("auto");
        testToXContent(true, null, null, null, List.of(), toolChoice, taskSettings(maxTokens, null, null, null), Strings.format("""
            {
                "model": "%s",
                "messages": [{"content": "%s", "role": "%s"}],
                "tool_choice": {"type": "auto"},
                "stream": true,
                "max_tokens": %d
            }
            """, MODEL_ID, INPUT_VALUE, ROLE_VALUE, maxTokens));
    }

    public void testToXContent_WithToolChoiceStringRequiredTranslatesToAnthropicAny() throws IOException {
        var maxTokens = 1024;
        var toolChoice = new ToolChoice.ToolChoiceString("required");
        testToXContent(true, null, null, null, List.of(), toolChoice, taskSettings(maxTokens, null, null, null), Strings.format("""
            {
                "model": "%s",
                "messages": [{"content": "%s", "role": "%s"}],
                "tool_choice": {"type": "any"},
                "stream": true,
                "max_tokens": %d
            }
            """, MODEL_ID, INPUT_VALUE, ROLE_VALUE, maxTokens));
    }

    public void testToXContent_WithSystemMessageExtractedToTopLevelField() throws IOException {
        var maxTokens = 1024;
        var messages = List.of(
            new Message(new ContentString("You are a pirate."), "system", null, null),
            new Message(new ContentString("Hello!"), "user", null, null)
        );
        testToXContentWithMessages(true, messages, taskSettings(maxTokens, null, null, null), Strings.format("""
            {
                "model": "%s",
                "system": [{"type": "text", "text": "You are a pirate."}],
                "messages": [{"content": "Hello!", "role": "user"}],
                "stream": true,
                "max_tokens": %d
            }
            """, MODEL_ID, maxTokens));
    }

    public void testToXContent_WithMultipleSystemMessagesWrittenAsTextBlockArray() throws IOException {
        var maxTokens = 1024;
        var messages = List.of(
            new Message(new ContentString("You are a pirate."), "system", null, null),
            new Message(new ContentString("Always respond in verse."), "system", null, null),
            new Message(new ContentString("Hello!"), "user", null, null)
        );
        testToXContentWithMessages(true, messages, taskSettings(maxTokens, null, null, null), Strings.format("""
            {
                "model": "%s",
                "system": [
                    {"type": "text", "text": "You are a pirate."},
                    {"type": "text", "text": "Always respond in verse."}
                ],
                "messages": [{"content": "Hello!", "role": "user"}],
                "stream": true,
                "max_tokens": %d
            }
            """, MODEL_ID, maxTokens));
    }

    public void testToXContent_WithStopSequencesTranslatedToAnthropicStopSequences() throws IOException {
        var maxTokens = 1024;
        var unifiedRequest = new UnifiedCompletionRequest(
            List.of(new Message(new ContentString(INPUT_VALUE), ROLE_VALUE, null, null)),
            null,
            null,
            List.of("END", "STOP"),
            null,
            null,
            null,
            null
        );
        var entity = new AnthropicUnifiedChatCompletionRequestEntity(
            new UnifiedChatInput(unifiedRequest, true),
            MODEL_ID,
            taskSettings(maxTokens, null, null, null)
        );
        XContentBuilder builder = JsonXContent.contentBuilder();
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertThat(Strings.toString(builder), is(XContentHelper.stripWhitespace(Strings.format("""
            {
                "model": "%s",
                "messages": [{"content": "%s", "role": "%s"}],
                "stop_sequences": ["END", "STOP"],
                "stream": true,
                "max_tokens": %d
            }
            """, MODEL_ID, INPUT_VALUE, ROLE_VALUE, maxTokens))));
    }

    public void testToXContent_WithToolChoiceStringUnknownValueRejected() throws IOException {
        var unsupportedValue = "unsupported_value";
        var toolChoice = new ToolChoice.ToolChoiceString(unsupportedValue);
        var entity = entity(true, null, null, null, List.of(), toolChoice, taskSettings(1024, null, null, null));
        var exception = expectThrows(ElasticsearchStatusException.class, () -> {
            try (var builder = JsonXContent.contentBuilder()) {
                entity.toXContent(builder, ToXContent.EMPTY_PARAMS);
            }
        });
        assertThat(exception.status(), is(RestStatus.BAD_REQUEST));
        assertThat(
            exception.getMessage(),
            is(Strings.format("Unsupported tool_choice value [%s] for the Anthropic chat completion API.", unsupportedValue))
        );
    }

    private static AnthropicChatCompletionTaskSettings taskSettings(
        int maxTokens,
        @Nullable Double temperature,
        @Nullable Double topP,
        @Nullable Integer topK
    ) {
        return new AnthropicChatCompletionTaskSettings(maxTokens, temperature, topP, topK);
    }

    private static AnthropicUnifiedChatCompletionRequestEntity entity(
        boolean stream,
        @Nullable Long requestMaxCompletionTokens,
        @Nullable Float requestTemperature,
        @Nullable Float requestTopP,
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

    private static void testToXContent(
        boolean stream,
        @Nullable Long requestMaxCompletionTokens,
        @Nullable Float requestTemperature,
        @Nullable Float requestTopP,
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

    private static void testToXContentWithMessages(
        boolean stream,
        List<Message> messages,
        AnthropicChatCompletionTaskSettings taskSettings,
        String expectedJson
    ) throws IOException {
        var unifiedRequest = new UnifiedCompletionRequest(messages, null, null, null, null, null, null, null);
        var entity = new AnthropicUnifiedChatCompletionRequestEntity(new UnifiedChatInput(unifiedRequest, stream), MODEL_ID, taskSettings);
        XContentBuilder builder = JsonXContent.contentBuilder();
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertThat(Strings.toString(builder), is(XContentHelper.stripWhitespace(expectedJson)));
    }
}
