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
import org.elasticsearch.inference.completion.ContentObject;
import org.elasticsearch.inference.completion.ContentObjects;
import org.elasticsearch.inference.completion.ContentString;
import org.elasticsearch.inference.completion.Message;
import org.elasticsearch.inference.completion.Tool;
import org.elasticsearch.inference.completion.ToolCall;
import org.elasticsearch.inference.completion.ToolChoice;
import org.elasticsearch.inference.completion.ToolChoice.ToolChoiceObject;
import org.elasticsearch.inference.completion.ToolChoice.ToolChoiceString;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class AnthropicToolUtilsTests extends ESTestCase {

    private static final String TOOL_NAME = "get_price";
    private static final String TOOL_DESCRIPTION = "Get the price";
    private static final Map<String, Object> INPUT_SCHEMA = Map.of(
        "type",
        "object",
        "properties",
        Map.of("item", Map.of("type", "string"))
    );

    public void testWriteToolChoice_translatesObjectToToolType() throws IOException {
        var toolChoice = new ToolChoiceObject("function", new ToolChoiceObject.FunctionField(TOOL_NAME));
        assertToolChoiceJson(toolChoice, Strings.format("""
            {
                "tool_choice": {
                    "type": "tool",
                    "name": "%s"
                }
            }
            """, TOOL_NAME));
    }

    public void testWriteToolChoice_translatesStringValues() throws IOException {
        assertStringToolChoiceTranslation("auto", "auto");
        assertStringToolChoiceTranslation("none", "none");
        // OpenAI's "required" maps to Anthropic's "any".
        assertStringToolChoiceTranslation("required", "any");
    }

    public void testWriteToolChoice_objectWithoutFunctionOmitsName() throws IOException {
        // Defensive branch: a tool_choice object with no function still produces a valid Anthropic {"type":"tool"}.
        assertToolChoiceJson(new ToolChoiceObject("function", null), """
            {
                "tool_choice": {
                    "type": "tool"
                }
            }
            """);
    }

    public void testWriteToolChoice_nullWritesNothing() throws IOException {
        assertToolChoiceJson(null, "{}");
    }

    public void testWriteToolChoice_unsupportedStringThrows() {
        var exception = expectThrows(ElasticsearchStatusException.class, () -> renderToolChoice(new ToolChoiceString("banana")));
        assertThat(exception.status(), is(RestStatus.BAD_REQUEST));
        assertThat(exception.getMessage(), is("Unsupported tool_choice value [banana] for the Anthropic chat completion API."));
    }

    public void testWriteTools_mapsFunctionToAnthropicShape() throws IOException {
        var tool = new Tool("function", new Tool.FunctionField(TOOL_DESCRIPTION, TOOL_NAME, INPUT_SCHEMA, null));
        assertToolsJson(List.of(tool), Strings.format("""
            {
                "tools": [
                    {
                        "name": "%s",
                        "description": "%s",
                        "input_schema": {
                            "type": "object",
                            "properties": {
                                "item": {
                                    "type": "string"
                                }
                            }
                        }
                    }
                ]
            }
            """, TOOL_NAME, TOOL_DESCRIPTION));
    }

    public void testWriteTools_defaultsInputSchemaWhenNoParameters() throws IOException {
        // Anthropic requires an object schema on every tool, so a parameterless tool still gets the minimal {"type":"object"} schema.
        var tool = new Tool("function", new Tool.FunctionField(TOOL_DESCRIPTION, TOOL_NAME, null, null));
        assertToolsJson(List.of(tool), Strings.format("""
            {
                "tools": [
                    {
                        "name": "%s",
                        "description": "%s",
                        "input_schema": {
                            "type": "object",
                            "properties": {}
                        }
                    }
                ]
            }
            """, TOOL_NAME, TOOL_DESCRIPTION));
    }

    public void testWriteTools_normalizesTypeAndPreservesSchemaKeywords() throws IOException {
        // Incoming "type" is forced to object; "properties"/"required" are copied and any other keyword is passed through.
        var parameters = Map.of(
            "type",
            "string",
            "properties",
            Map.of("item", Map.of("type", "string")),
            "required",
            List.of("item"),
            "additionalProperties",
            false
        );
        var tool = new Tool("function", new Tool.FunctionField(TOOL_DESCRIPTION, TOOL_NAME, parameters, null));
        assertToolsJson(List.of(tool), Strings.format("""
            {
                "tools": [
                    {
                        "name": "%s",
                        "description": "%s",
                        "input_schema": {
                            "type": "object",
                            "properties": {
                                "item": {
                                    "type": "string"
                                }
                            },
                            "required": ["item"],
                            "additionalProperties": false
                        }
                    }
                ]
            }
            """, TOOL_NAME, TOOL_DESCRIPTION));
    }

    public void testWriteTools_serializesMultipleTools() throws IOException {
        var first = new Tool("function", new Tool.FunctionField("First", "first", INPUT_SCHEMA, null));
        var second = new Tool("function", new Tool.FunctionField("Second", "second", null, null));
        assertToolsJson(List.of(first, second), """
            {
                "tools": [
                    {
                        "name": "first",
                        "description": "First",
                        "input_schema": {
                            "type": "object",
                            "properties": {
                                "item": {
                                    "type": "string"
                                }
                            }
                        }
                    },
                    {
                        "name": "second",
                        "description": "Second",
                        "input_schema": {
                            "type": "object",
                            "properties": {}
                        }
                    }
                ]
            }
            """);
    }

    public void testWriteTools_nullOrEmptyWritesNothing() throws IOException {
        assertToolsJson(null, "{}");
        assertToolsJson(List.of(), "{}");
    }

    public void testWriteTools_ignoresStrictField() throws IOException {
        // The OpenAI "strict" field has no Anthropic equivalent, so it is silently dropped rather than rejected.
        var tool = new Tool("function", new Tool.FunctionField(TOOL_DESCRIPTION, TOOL_NAME, INPUT_SCHEMA, randomBoolean()));
        assertToolsJson(List.of(tool), Strings.format("""
            {
                "tools": [
                    {
                        "name": "%s",
                        "description": "%s",
                        "input_schema": {
                            "type": "object",
                            "properties": {
                                "item": {
                                    "type": "string"
                                }
                            }
                        }
                    }
                ]
            }
            """, TOOL_NAME, TOOL_DESCRIPTION));
    }

    public void testWriteMessages_translatesAssistantToolCallsToToolUseBlocks() throws IOException {
        var toolCall = new ToolCall("call_1", new ToolCall.FunctionField("{\"location\":\"San Francisco\"}", "get_weather"), "function");
        var message = new Message(new ContentString(""), "assistant", null, List.of(toolCall));
        assertMessagesJson(List.of(message), """
            {
                "messages": [
                    {
                        "role": "assistant",
                        "content": [
                            {
                                "type": "tool_use",
                                "id": "call_1",
                                "name": "get_weather",
                                "input": {"location": "San Francisco"}
                            }
                        ]
                    }
                ]
            }
            """);
    }

    public void testWriteMessages_translatesToolResultToUserMessage() throws IOException {
        var message = new Message(new ContentString("72F and sunny"), "tool", "call_1", null);
        assertMessagesJson(List.of(message), """
            {
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "tool_result",
                                "tool_use_id": "call_1",
                                "content": [
                                    {
                                        "type": "text",
                                        "text": "72F and sunny"
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
            """);
    }

    public void testWriteMessages_translatesToolResultContentObjectsToTextBlocks() throws IOException {
        // OpenAI also accepts a tool message whose content is an array of text objects; each becomes an Anthropic text block.
        var content = new ContentObjects(
            List.of(new ContentObject.ContentObjectText("72F and sunny"), new ContentObject.ContentObjectText("Humidity is 40%"))
        );
        var message = new Message(content, "tool", "call_1", null);
        assertMessagesJson(List.of(message), """
            {
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "tool_result",
                                "tool_use_id": "call_1",
                                "content": [
                                    {
                                        "type": "text",
                                        "text": "72F and sunny"
                                    },
                                    {
                                        "type": "text",
                                        "text": "Humidity is 40%"
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
            """);
    }

    public void testWriteMessages_toolResultWithNonTextContentThrows() {
        var imageUrl = new ContentObject.ContentObjectImage.ContentObjectImageUrl("https://example.com/image.png", null);
        var content = new ContentObjects(List.of(new ContentObject.ContentObjectImage(imageUrl)));
        var message = new Message(content, "tool", "call_1", null);
        var exception = expectThrows(ElasticsearchStatusException.class, () -> renderMessages(List.of(message)));
        assertThat(exception.status(), is(RestStatus.BAD_REQUEST));
        assertThat(
            exception.getMessage(),
            is("Unsupported content type [image_url] in a tool message for the Anthropic chat completion API.")
        );
    }

    public void testWriteMessages_toolResultWithoutToolCallIdThrows() {
        // Anthropic requires tool_use_id on every tool_result block, so a tool message must carry a tool_call_id.
        var message = new Message(new ContentString("72F and sunny"), "tool", null, null);
        var exception = expectThrows(ElasticsearchStatusException.class, () -> renderMessages(List.of(message)));
        assertThat(exception.status(), is(RestStatus.BAD_REQUEST));
        assertThat(exception.getMessage(), is("Field [tool_call_id] is required in a tool message for the Anthropic chat completion API."));
    }

    public void testWriteMessages_emitsLeadingTextBlockWhenAssistantHasContent() throws IOException {
        var toolCall = new ToolCall("call_1", new ToolCall.FunctionField("{\"location\":\"San Francisco\"}", "get_weather"), "function");
        var message = new Message(new ContentString("Let me check the weather."), "assistant", null, List.of(toolCall));
        assertMessagesJson(List.of(message), """
            {
                "messages": [
                    {
                        "role": "assistant",
                        "content": [
                            {
                                "type": "text",
                                "text": "Let me check the weather."
                            },
                            {
                                "type": "tool_use",
                                "id": "call_1",
                                "name": "get_weather",
                                "input": {"location": "San Francisco"}
                            }
                        ]
                    }
                ]
            }
            """);
    }

    public void testWriteMessages_blankArgumentsYieldEmptyInputObject() throws IOException {
        // A parameterless tool call carries no arguments; Anthropic still requires an "input" object, so we emit an empty one.
        var arguments = randomBoolean() ? null : "";
        var toolCall = new ToolCall("call_1", new ToolCall.FunctionField(arguments, "get_time"), "function");
        var message = new Message(new ContentString(""), "assistant", null, List.of(toolCall));
        assertMessagesJson(List.of(message), """
            {
                "messages": [
                    {
                        "role": "assistant",
                        "content": [
                            {
                                "type": "tool_use",
                                "id": "call_1",
                                "name": "get_time",
                                "input": {}
                            }
                        ]
                    }
                ]
            }
            """);
    }

    public void testWriteMessages_passesPlainMessagesThrough() throws IOException {
        var message = new Message(new ContentString("Hello!"), "user", null, null);
        assertMessagesJson(List.of(message), """
            {
                "messages": [
                    {
                        "content": "Hello!",
                        "role": "user"
                    }
                ]
            }
            """);
    }

    public void testWriteMessages_multiTurnToolConversationHasNoUnifiedToolFields() throws IOException {
        // Regression guard for the second-turn 400: the serialized request must not contain the unified "role":"tool" message or the
        // "tool_calls" field, and must contain the Anthropic tool_use / tool_result blocks instead.
        var toolCall = new ToolCall("call_1", new ToolCall.FunctionField("{\"location\":\"San Francisco\"}", "get_weather"), "function");
        var messages = List.of(
            new Message(new ContentString("What is the weather in San Francisco?"), "user", null, null),
            new Message(new ContentString(""), "assistant", null, List.of(toolCall)),
            new Message(new ContentString("72F and sunny"), "tool", "call_1", null)
        );
        var actual = renderMessages(messages);
        assertThat(actual, not(containsString("\"role\":\"tool\"")));
        assertThat(actual, not(containsString("\"tool_calls\"")));
        assertThat(actual, containsString("\"type\":\"tool_use\""));
        assertThat(actual, containsString("\"type\":\"tool_result\""));
        assertThat(actual, containsString("\"tool_use_id\":\"call_1\""));
    }

    private static void assertMessagesJson(List<Message> messages, String expectedJson) throws IOException {
        assertThat(renderMessages(messages), is(XContentHelper.stripWhitespace(expectedJson)));
    }

    private static String renderMessages(List<Message> messages) throws IOException {
        try (var builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            AnthropicToolUtils.writeMessages(builder, messages);
            builder.endObject();
            return Strings.toString(builder);
        }
    }

    private static void assertStringToolChoiceTranslation(String openAiValue, String anthropicType) throws IOException {
        assertToolChoiceJson(new ToolChoiceString(openAiValue), Strings.format("""
            {
                "tool_choice": {
                    "type": "%s"
                }
            }
            """, anthropicType));
    }

    private static void assertToolChoiceJson(ToolChoice toolChoice, String expectedJson) throws IOException {
        assertThat(renderToolChoice(toolChoice), is(XContentHelper.stripWhitespace(expectedJson)));
    }

    private static String renderToolChoice(ToolChoice toolChoice) throws IOException {
        try (var builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            AnthropicToolUtils.writeToolChoice(builder, toolChoice);
            builder.endObject();
            return Strings.toString(builder);
        }
    }

    private static void assertToolsJson(List<Tool> tools, String expectedJson) throws IOException {
        assertThat(renderTools(tools), is(XContentHelper.stripWhitespace(expectedJson)));
    }

    private static String renderTools(List<Tool> tools) throws IOException {
        try (var builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            AnthropicToolUtils.writeTools(builder, tools);
            builder.endObject();
            return Strings.toString(builder);
        }
    }
}
