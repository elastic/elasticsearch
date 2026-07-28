/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.anthropic.request;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.completion.Tool;
import org.elasticsearch.inference.completion.ToolChoice;
import org.elasticsearch.inference.completion.ToolChoice.ToolChoiceObject;
import org.elasticsearch.inference.completion.ToolChoice.ToolChoiceString;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.DESCRIPTION_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.NAME_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.TOOL_CHOICE_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.TOOL_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.TYPE_FIELD;

/**
 * Serializes the tool-calling portion of a unified {@code chat_completion} request into the shape expected by the
 * <a href="https://platform.claude.com/docs/en/api/messages/create">Anthropic Messages API</a>.
 *
 * <p>The unified API accepts {@code tool_choice} and {@code tools} in OpenAI's vocabulary, which differs from Anthropic's.
 * These helpers perform that translation so it is implemented in exactly one place. Both the direct Anthropic service and the
 * Google Model Garden Anthropic provider (which proxies the same Messages API) share this logic.
 */
public final class AnthropicToolUtils {

    private static final String TOOL_CHOICE_TOOL_TYPE = "tool";
    private static final String INPUT_SCHEMA_FIELD = "input_schema";
    private static final String OBJECT_TYPE = "object";
    private static final String PROPERTIES_FIELD = "properties";
    private static final String REQUIRED_FIELD = "required";

    private AnthropicToolUtils() {}

    /**
     * Writes the {@code tool_choice} field, translating OpenAI's representation into Anthropic's. Does nothing when
     * {@code toolChoice} is {@code null}.
     *
     * <ul>
     *     <li>Object {@code {"type":"function","function":{"name":"function_name"}}} becomes
     *         {@code {"type":"tool","name":"function_name"}}.</li>
     *     <li>String {@code "auto"}/{@code "none"} are passed through and {@code "required"} maps to Anthropic's {@code "any"}.</li>
     * </ul>
     *
     * @throws ElasticsearchStatusException if a string value is not one Anthropic understands.
     */
    public static void writeToolChoice(XContentBuilder builder, ToolChoice toolChoice) throws IOException {
        if (toolChoice == null) {
            return;
        }
        if (toolChoice instanceof ToolChoiceObject toolChoiceObject) {
            builder.startObject(TOOL_CHOICE_FIELD);
            builder.field(TYPE_FIELD, TOOL_CHOICE_TOOL_TYPE);
            if (toolChoiceObject.function() != null) {
                builder.field(NAME_FIELD, toolChoiceObject.function().name());
            }
            builder.endObject();
        } else if (toolChoice instanceof ToolChoiceString toolChoiceString) {
            String anthropicType = switch (toolChoiceString.value()) {
                case "none" -> "none";
                case "auto" -> "auto";
                case "required" -> "any";
                default -> throw new ElasticsearchStatusException(
                    Strings.format("Unsupported tool_choice value [%s] for the Anthropic chat completion API.", toolChoiceString.value()),
                    RestStatus.BAD_REQUEST
                );
            };
            builder.startObject(TOOL_CHOICE_FIELD);
            builder.field(TYPE_FIELD, anthropicType);
            builder.endObject();
        }
    }

    /**
     * Writes the {@code tools} array, mapping each unified tool's function definition onto Anthropic's
     * {@code name}/{@code description}/{@code input_schema} shape. Does nothing when {@code tools} is {@code null} or empty.
     *
     * <p>The OpenAI {@code strict} field has no Anthropic equivalent and is silently dropped (rather than rejected) so that
     * OpenAI-shaped requests remain drop-in compatible, for consistency with other inference services.
     */
    public static void writeTools(XContentBuilder builder, List<Tool> tools) throws IOException {
        if (tools == null || tools.isEmpty()) {
            return;
        }
        builder.startArray(TOOL_FIELD);
        for (var tool : tools) {
            var function = tool.function();
            builder.startObject();
            builder.field(NAME_FIELD, function.name());
            builder.field(DESCRIPTION_FIELD, function.description());
            writeInputSchema(builder, function.parameters());
            builder.endObject();
        }
        builder.endArray();
    }

    /**
     * Writes a tool's {@code input_schema}. Anthropic requires a JSON-Schema object on every tool, so this always emits an object
     * schema: {@code type} is forced to {@code "object"}, {@code properties} defaults to an empty object, {@code required} is
     * copied through when present, and any remaining JSON-Schema keywords are passed through unchanged. When {@code parameters}
     * is {@code null} (a parameterless tool) the minimal valid schema {@code {"type":"object","properties":{}}} is emitted.
     */
    private static void writeInputSchema(XContentBuilder builder, Map<String, Object> parameters) throws IOException {
        builder.startObject(INPUT_SCHEMA_FIELD);
        builder.field(TYPE_FIELD, OBJECT_TYPE);
        if (parameters == null) {
            builder.startObject(PROPERTIES_FIELD).endObject();
            builder.endObject();
            return;
        }
        var properties = parameters.get(PROPERTIES_FIELD);
        if (properties != null) {
            builder.field(PROPERTIES_FIELD, properties);
        } else {
            builder.startObject(PROPERTIES_FIELD).endObject();
        }
        var required = parameters.get(REQUIRED_FIELD);
        if (required != null) {
            builder.field(REQUIRED_FIELD, required);
        }
        // Preserve any other JSON-Schema keywords (e.g. additionalProperties, $defs); the incoming "type" is normalized to object.
        for (var entry : parameters.entrySet()) {
            var key = entry.getKey();
            if (TYPE_FIELD.equals(key) || PROPERTIES_FIELD.equals(key) || REQUIRED_FIELD.equals(key)) {
                continue;
            }
            builder.field(key, entry.getValue());
        }
        builder.endObject();
    }
}
