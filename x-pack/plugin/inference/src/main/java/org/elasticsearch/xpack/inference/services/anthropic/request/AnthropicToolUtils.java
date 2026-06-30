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

    private AnthropicToolUtils() {}

    /**
     * Writes the {@code tool_choice} field, translating OpenAI's representation into Anthropic's. Does nothing when
     * {@code toolChoice} is {@code null}.
     *
     * <ul>
     *     <li>Object {@code {"type":"function","function":{"name":"function_name"}}} becomes {@code {"type":"tool","name":"function_name"}}.</li>
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
     * @throws ElasticsearchStatusException if a tool sets the {@code strict} field, which Anthropic does not support.
     */
    public static void writeTools(XContentBuilder builder, List<Tool> tools) throws IOException {
        if (tools == null || tools.isEmpty()) {
            return;
        }
        builder.startArray(TOOL_FIELD);
        for (var tool : tools) {
            var function = tool.function();
            if (function.strict() != null) {
                throw new ElasticsearchStatusException(
                    "The [strict] field in tool function definitions is not supported by the Anthropic chat completion API.",
                    RestStatus.BAD_REQUEST
                );
            }
            builder.startObject();
            builder.field(NAME_FIELD, function.name());
            builder.field(DESCRIPTION_FIELD, function.description());
            var parameters = function.parameters();
            if (parameters != null && parameters.isEmpty() == false) {
                builder.field(INPUT_SCHEMA_FIELD, parameters);
            }
            builder.endObject();
        }
        builder.endArray();
    }
}
