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
import java.nio.charset.StandardCharsets;
import java.util.Base64;
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
        // All empty content shapes yield the same output: tool_use blocks with no leading text block.
        var content = randomFrom(new ContentString(""), new ContentObjects(List.of()), null);
        var message = new Message(content, "assistant", null, List.of(toolCall));
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

    public void testWriteMessages_translatesToolResultImageToImageBlock() throws IOException {
        var imageUrl = new ContentObject.ContentObjectImage.ContentObjectImageUrl("data:image/png;base64,iVBORw==", null);
        var content = new ContentObjects(List.of(new ContentObject.ContentObjectImage(imageUrl)));
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
                                        "type": "image",
                                        "source": {
                                            "type": "base64",
                                            "media_type": "image/png",
                                            "data": "iVBORw=="
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
            """);
    }

    public void testWriteMessages_translatesUserMultimodalContentToAnthropicBlocks() throws IOException {
        // The non-standard "image/jpg" media type is normalized to "image/jpeg", which is what Anthropic accepts.
        var text = new ContentObject.ContentObjectText("What do the image and the report show?");
        var image = new ContentObject.ContentObjectImage(
            new ContentObject.ContentObjectImage.ContentObjectImageUrl("data:image/jpg;base64,iVBORw==", null)
        );
        var file = new ContentObject.ContentObjectFile(
            new ContentObject.ContentObjectFile.ContentObjectFileFields("data:application/pdf;base64,JVBERg==", null, "report.pdf")
        );
        var message = new Message(new ContentObjects(List.of(text, image, file)), "user", null, null);
        assertMessagesJson(List.of(message), """
            {
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": "What do the image and the report show?"
                            },
                            {
                                "type": "image",
                                "source": {
                                    "type": "base64",
                                    "media_type": "image/jpeg",
                                    "data": "iVBORw=="
                                }
                            },
                            {
                                "type": "document",
                                "source": {
                                    "type": "base64",
                                    "media_type": "application/pdf",
                                    "data": "JVBERg=="
                                },
                                "title": "report.pdf"
                            }
                        ]
                    }
                ]
            }
            """);
    }

    public void testWriteMessages_translatesHttpImageUrlToUrlSource() throws IOException {
        var image = new ContentObject.ContentObjectImage(
            new ContentObject.ContentObjectImage.ContentObjectImageUrl("https://example.com/image.png", null)
        );
        var message = new Message(new ContentObjects(List.of(image)), "user", null, null);
        assertMessagesJson(List.of(message), """
            {
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "image",
                                "source": {
                                    "type": "url",
                                    "url": "https://example.com/image.png"
                                }
                            }
                        ]
                    }
                ]
            }
            """);
    }

    public void testWriteMessages_translatesPlainTextFileToTextSource() throws IOException {
        // A text/plain document maps onto Anthropic's text source, which carries the decoded text rather than base64. The declared
        // media type's RFC 2397 parameters (";charset=utf-8") are stripped.
        var encoded = Base64.getEncoder().encodeToString("72F and sunny".getBytes(StandardCharsets.UTF_8));
        var file = new ContentObject.ContentObjectFile(
            new ContentObject.ContentObjectFile.ContentObjectFileFields(
                "data:text/plain;charset=utf-8;base64," + encoded,
                null,
                "weather.txt"
            )
        );
        var message = new Message(new ContentObjects(List.of(file)), "user", null, null);
        assertMessagesJson(List.of(message), """
            {
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "document",
                                "source": {
                                    "type": "text",
                                    "media_type": "text/plain",
                                    "data": "72F and sunny"
                                },
                                "title": "weather.txt"
                            }
                        ]
                    }
                ]
            }
            """);
    }

    public void testWriteMessages_imageWithBareBase64Throws() {
        // Bare base64 carries no media type, which Anthropic requires; the value must be a data URI declaring it.
        var image = new ContentObject.ContentObjectImage(new ContentObject.ContentObjectImage.ContentObjectImageUrl("iVBORw==", null));
        var message = new Message(new ContentObjects(List.of(image)), "user", null, null);
        var exception = expectThrows(ElasticsearchStatusException.class, () -> renderMessages(List.of(message)));
        assertThat(exception.status(), is(RestStatus.BAD_REQUEST));
        assertThat(
            exception.getMessage(),
            is(
                "Image URLs must be HTTP(S) URLs or base64 data URIs with the format [data:{MIME-type};base64,...] "
                    + "for the Anthropic chat completion API."
            )
        );
    }

    public void testWriteMessages_fileWithoutDataUriThrows() {
        var file = new ContentObject.ContentObjectFile(
            new ContentObject.ContentObjectFile.ContentObjectFileFields("JVBERg==", null, "report.pdf")
        );
        var message = new Message(new ContentObjects(List.of(file)), "user", null, null);
        var exception = expectThrows(ElasticsearchStatusException.class, () -> renderMessages(List.of(message)));
        assertThat(exception.status(), is(RestStatus.BAD_REQUEST));
        assertThat(
            exception.getMessage(),
            is(
                "File data must be a base64 data URI with the format [data:{MIME-type};base64,...] "
                    + "for the Anthropic chat completion API."
            )
        );
    }

    public void testWriteMessages_fileWithoutFileDataThrows() {
        var file = new ContentObject.ContentObjectFile(new ContentObject.ContentObjectFile.ContentObjectFileFields(null, null, "a.pdf"));
        var message = new Message(new ContentObjects(List.of(file)), "user", null, null);
        var exception = expectThrows(ElasticsearchStatusException.class, () -> renderMessages(List.of(message)));
        assertThat(exception.status(), is(RestStatus.BAD_REQUEST));
        assertThat(exception.getMessage(), is("File content requires [file_data] for the Anthropic chat completion API."));
    }

    public void testWriteMessages_fileWithUnsupportedMediaTypeThrows() {
        var file = new ContentObject.ContentObjectFile(
            new ContentObject.ContentObjectFile.ContentObjectFileFields("data:image/png;base64,iVBORw==", null, "image.png")
        );
        var message = new Message(new ContentObjects(List.of(file)), "user", null, null);
        var exception = expectThrows(ElasticsearchStatusException.class, () -> renderMessages(List.of(message)));
        assertThat(exception.status(), is(RestStatus.BAD_REQUEST));
        assertThat(
            exception.getMessage(),
            is(
                "Unsupported file media type [image/png] for the Anthropic chat completion API; "
                    + "supported types are [application/pdf, text/plain]."
            )
        );
    }

    public void testWriteMessages_fileWithInvalidBase64PayloadThrows() {
        var file = new ContentObject.ContentObjectFile(
            new ContentObject.ContentObjectFile.ContentObjectFileFields("data:text/plain;base64,!!!", null, "notes.txt")
        );
        var message = new Message(new ContentObjects(List.of(file)), "user", null, null);
        var exception = expectThrows(ElasticsearchStatusException.class, () -> renderMessages(List.of(message)));
        assertThat(exception.status(), is(RestStatus.BAD_REQUEST));
        assertThat(exception.getMessage(), is("Invalid base64 payload in the file data URI."));
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

    public void testWriteMessages_emitsLeadingTextBlockFromAssistantContentObjects() throws IOException {
        // A tool-calling assistant message may carry its text as content objects; their text items are concatenated into the
        // single leading text block, matching the ContentString shape.
        var content = new ContentObjects(
            List.of(new ContentObject.ContentObjectText("Let me check "), new ContentObject.ContentObjectText("the weather."))
        );
        var toolCall = new ToolCall("call_1", new ToolCall.FunctionField("{\"location\":\"San Francisco\"}", "get_weather"), "function");
        var message = new Message(content, "assistant", null, List.of(toolCall));
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

    public void testWriteMessages_wrapsAssistantStringContentInTextBlock() throws IOException {
        // An assistant message with no tool calls also gets the array-shaped content.
        var message = new Message(new ContentString("The weather is sunny."), "assistant", null, null);
        assertMessagesJson(List.of(message), """
            {
                "messages": [
                    {
                        "role": "assistant",
                        "content": [
                            {
                                "type": "text",
                                "text": "The weather is sunny."
                            }
                        ]
                    }
                ]
            }
            """);
    }

    public void testWriteMessages_emptyUserContentBecomesEmptyTextBlock() throws IOException {
        // Anthropic rejects messages without content, so empty or absent user content is normalized to a single empty text block.
        var expectedJson = """
            {
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": ""
                            }
                        ]
                    }
                ]
            }
            """;
        assertMessagesJson(List.of(new Message(new ContentString(""), "user", null, null)), expectedJson);
        assertMessagesJson(List.of(new Message(new ContentObjects(List.of()), "user", null, null)), expectedJson);
        assertMessagesJson(List.of(new Message(null, "user", null, null)), expectedJson);
    }

    public void testWriteMessages_emptyAssistantContentBecomesEmptyTextBlock() throws IOException {
        // An assistant message with no tool calls and empty or absent content is normalized to a single empty text block, which is
        // a fallback when no content blocks were produced.
        var toolCalls = randomBoolean() ? null : List.<ToolCall>of();
        var expectedJson = """
            {
                "messages": [
                    {
                        "role": "assistant",
                        "content": [
                            {
                                "type": "text",
                                "text": ""
                            }
                        ]
                    }
                ]
            }
            """;
        assertMessagesJson(List.of(new Message(new ContentString(""), "assistant", null, toolCalls)), expectedJson);
        assertMessagesJson(List.of(new Message(new ContentObjects(List.of()), "assistant", null, toolCalls)), expectedJson);
        assertMessagesJson(List.of(new Message(null, "assistant", null, toolCalls)), expectedJson);
    }

    public void testWriteMessages_unsupportedRoleThrows() {
        // Only user/assistant/tool messages can be translated; system messages must be extracted into the top-level "system"
        // field before writeMessages is called.
        var role = randomFrom("system", "developer");
        var message = new Message(new ContentString("You are a helpful assistant."), role, null, null);
        var exception = expectThrows(ElasticsearchStatusException.class, () -> renderMessages(List.of(message)));
        assertThat(exception.status(), is(RestStatus.BAD_REQUEST));
        assertThat(exception.getMessage(), is(Strings.format("Unsupported role [%s] for the Anthropic chat completion API.", role)));
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

    public void testWriteMessages_wrapsUserStringContentInTextBlock() throws IOException {
        // Plain-string content is wrapped in a single text block so every message carries array-shaped content.
        var message = new Message(new ContentString("Hello!"), "user", null, null);
        assertMessagesJson(List.of(message), """
            {
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": "Hello!"
                            }
                        ]
                    }
                ]
            }
            """);
    }

    public void testExtractText_returnsStringContent() {
        assertThat(AnthropicToolUtils.extractText(new ContentString("Hello!")), is("Hello!"));
    }

    public void testExtractText_concatenatesTextObjectsAndIgnoresOthers() {
        var image = new ContentObject.ContentObjectImage(
            new ContentObject.ContentObjectImage.ContentObjectImageUrl("https://example.com/image.png", null)
        );
        var content = new ContentObjects(
            List.of(new ContentObject.ContentObjectText("Hello"), image, new ContentObject.ContentObjectText(" world"))
        );
        assertThat(AnthropicToolUtils.extractText(content), is("Hello world"));
    }

    public void testExtractText_emptyOrAbsentContentYieldsEmptyString() {
        assertThat(AnthropicToolUtils.extractText(null), is(""));
        assertThat(AnthropicToolUtils.extractText(new ContentString("")), is(""));
        assertThat(AnthropicToolUtils.extractText(new ContentObjects(List.of())), is(""));
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
