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
import org.elasticsearch.inference.UnifiedCompletionRequest.Tool;
import org.elasticsearch.inference.UnifiedCompletionRequest.ToolChoice;
import org.elasticsearch.inference.UnifiedCompletionRequest.ToolChoiceObject;
import org.elasticsearch.inference.UnifiedCompletionRequest.ToolChoiceString;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;

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
