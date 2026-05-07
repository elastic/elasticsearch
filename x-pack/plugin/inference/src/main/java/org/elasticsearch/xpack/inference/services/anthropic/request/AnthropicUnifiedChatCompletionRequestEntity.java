/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.anthropic.request;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.inference.completion.ToolChoice.ToolChoiceObject;
import org.elasticsearch.inference.completion.ToolChoice.ToolChoiceString;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.anthropic.completion.AnthropicChatCompletionTaskSettings;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.DESCRIPTION_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.MAX_TOKENS_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.MESSAGES_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.MODEL_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.NAME_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.TEMPERATURE_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.TOOL_CHOICE_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.TOOL_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.TOP_P_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.TYPE_FIELD;

/**
 * Builds the request body for the Anthropic Messages API
 * (<a href="https://docs.anthropic.com/en/api/messages">/v1/messages</a>) when invoked through the unified
 * {@code chat_completion} task type. The output mirrors the Anthropic-shaped body produced by
 * {@code GoogleModelGardenAnthropicChatCompletionRequestEntity} but is tailored for the direct Anthropic API
 * (no {@code anthropic_version} body field; that is sent as the {@code anthropic-version} HTTP header).
 *
 * <p>Anthropic requires {@code max_tokens} on every request. The value is taken from
 * {@link UnifiedCompletionRequest#maxCompletionTokens()} when supplied by the caller, otherwise from the
 * stored {@link AnthropicChatCompletionTaskSettings#maxTokens()} which is required at endpoint creation.
 */
public class AnthropicUnifiedChatCompletionRequestEntity implements ToXContentObject {

    private static final String STREAM_FIELD = "stream";
    private static final String INPUT_SCHEMA_FIELD = "input_schema";

    private final UnifiedCompletionRequest unifiedRequest;
    private final boolean stream;
    private final String modelId;
    private final AnthropicChatCompletionTaskSettings taskSettings;

    public AnthropicUnifiedChatCompletionRequestEntity(
        UnifiedChatInput unifiedChatInput,
        String modelId,
        AnthropicChatCompletionTaskSettings taskSettings
    ) {
        this(
            Objects.requireNonNull(unifiedChatInput).getRequest(),
            Objects.requireNonNull(unifiedChatInput).stream(),
            modelId,
            taskSettings
        );
    }

    public AnthropicUnifiedChatCompletionRequestEntity(
        UnifiedCompletionRequest unifiedRequest,
        boolean stream,
        String modelId,
        AnthropicChatCompletionTaskSettings taskSettings
    ) {
        this.unifiedRequest = Objects.requireNonNull(unifiedRequest);
        this.stream = stream;
        this.modelId = Objects.requireNonNull(modelId);
        this.taskSettings = Objects.requireNonNull(taskSettings);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.field(MODEL_FIELD, modelId);
        builder.field(MESSAGES_FIELD, unifiedRequest.messages());

        if (unifiedRequest.temperature() != null) {
            builder.field(TEMPERATURE_FIELD, unifiedRequest.temperature());
        } else if (taskSettings.temperature() != null) {
            builder.field(TEMPERATURE_FIELD, taskSettings.temperature());
        }

        if (unifiedRequest.topP() != null) {
            builder.field(TOP_P_FIELD, unifiedRequest.topP());
        } else if (taskSettings.topP() != null) {
            builder.field(TOP_P_FIELD, taskSettings.topP());
        }

        var toolChoice = unifiedRequest.toolChoice();
        if (toolChoice != null) {
            if (toolChoice instanceof ToolChoiceObject toolChoiceObject) {
                builder.startObject(TOOL_CHOICE_FIELD);
                builder.field(TYPE_FIELD, toolChoiceObject.type());
                builder.endObject();
            } else if (toolChoice instanceof ToolChoiceString) {
                throw new ElasticsearchStatusException(
                    "Tool choice value is not supported as a string by the Anthropic chat completion API.",
                    RestStatus.BAD_REQUEST
                );
            }
        }

        var tools = unifiedRequest.tools();
        if (tools != null && tools.isEmpty() == false) {
            builder.startArray(TOOL_FIELD);
            for (var tool : tools) {
                var function = tool.function();
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

        builder.field(STREAM_FIELD, stream);

        final long maxTokens = unifiedRequest.maxCompletionTokens() != null
            ? unifiedRequest.maxCompletionTokens()
            : taskSettings.maxTokens();
        builder.field(MAX_TOKENS_FIELD, maxTokens);

        builder.endObject();
        return builder;
    }
}
