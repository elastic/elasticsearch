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
import org.elasticsearch.inference.completion.Tool;
import org.elasticsearch.inference.completion.ToolChoice;
import org.elasticsearch.inference.completion.ToolChoice.ToolChoiceObject;
import org.elasticsearch.inference.completion.ToolChoice.ToolChoiceString;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class AnthropicToolUtilsTests extends ESTestCase {

    public void testWriteToolChoice_translatesObjectToToolType() throws IOException {
        var toolChoice = new ToolChoiceObject("function", new ToolChoiceObject.FunctionField("get_price"));
        assertToolChoiceJson(toolChoice, """
            {"tool_choice":{"type":"tool","name":"get_price"}}""");
    }

    public void testWriteToolChoice_translatesStringValues() throws IOException {
        assertToolChoiceJson(new ToolChoiceString("auto"), """
            {"tool_choice":{"type":"auto"}}""");
        assertToolChoiceJson(new ToolChoiceString("none"), """
            {"tool_choice":{"type":"none"}}""");
        // OpenAI's "required" maps to Anthropic's "any".
        assertToolChoiceJson(new ToolChoiceString("required"), """
            {"tool_choice":{"type":"any"}}""");
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
        var tool = new Tool("function", new Tool.FunctionField("Get the price", "get_price", Map.of("type", "object"), null));
        assertToolsJson(List.of(tool), """
            {"tools":[{"name":"get_price","description":"Get the price","input_schema":{"type":"object"}}]}""");
    }

    public void testWriteTools_nullOrEmptyWritesNothing() throws IOException {
        assertToolsJson(null, "{}");
        assertToolsJson(List.of(), "{}");
    }

    public void testWriteTools_strictFieldThrows() {
        var tool = new Tool("function", new Tool.FunctionField("Get the price", "get_price", Map.of("type", "object"), randomBoolean()));
        var exception = expectThrows(ElasticsearchStatusException.class, () -> renderTools(List.of(tool)));
        assertThat(exception.status(), is(RestStatus.BAD_REQUEST));
        assertThat(
            exception.getMessage(),
            is("The [strict] field in tool function definitions is not supported by the Anthropic chat completion API.")
        );
    }

    private static void assertToolChoiceJson(ToolChoice toolChoice, String expectedJson) throws IOException {
        assertEquals(XContentHelper.stripWhitespace(expectedJson), renderToolChoice(toolChoice));
    }

    private static String renderToolChoice(ToolChoice toolChoice) throws IOException {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            AnthropicToolUtils.writeToolChoice(builder, toolChoice);
            builder.endObject();
            return Strings.toString(builder);
        }
    }

    private static void assertToolsJson(List<Tool> tools, String expectedJson) throws IOException {
        assertEquals(XContentHelper.stripWhitespace(expectedJson), renderTools(tools));
    }

    private static String renderTools(List<Tool> tools) throws IOException {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            AnthropicToolUtils.writeTools(builder, tools);
            builder.endObject();
            return Strings.toString(builder);
        }
    }
}
