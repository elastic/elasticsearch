/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.request.completion;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.googlevertexai.completion.GoogleVertexAiChatCompletionTaskSettings;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.inference.UnifiedCompletionRequest.DESCRIPTION_FIELD;
import static org.elasticsearch.inference.UnifiedCompletionRequest.MESSAGES_FIELD;
import static org.elasticsearch.inference.UnifiedCompletionRequest.NAME_FIELD;
import static org.elasticsearch.inference.UnifiedCompletionRequest.TEMPERATURE_FIELD;
import static org.elasticsearch.inference.UnifiedCompletionRequest.TOOL_CHOICE_FIELD;
import static org.elasticsearch.inference.UnifiedCompletionRequest.TOOL_FIELD;
import static org.elasticsearch.inference.UnifiedCompletionRequest.TOP_P_FIELD;
import static org.elasticsearch.inference.UnifiedCompletionRequest.TYPE_FIELD;
import static org.elasticsearch.xpack.inference.services.mistral.MistralConstants.MAX_TOKENS_FIELD;

/**
 * Request entity for Google Model Garden Anthropic Chat Completion API.
 */
public class GoogleModelGardenAnthropicChatCompletionRequestEntity implements ToXContentObject {

    private static final String ANTHROPIC_VERSION = "anthropic_version";
    // Anthropic requires this specific version for Google Model Garden according to their documentation.
    // https://console.cloud.google.com/vertex-ai/publishers/anthropic/model-garden/claude-3-5-haiku
    private static final String VERTEX_2023_10_16 = "vertex-2023-10-16";
    private static final String STREAM_FIELD = "stream";
    private static final String INPUT_SCHEMA_FIELD = "input_schema";
    public static final int DEFAULT_MAX_TOKENS = 1024;

    private final UnifiedCompletionRequest unifiedRequest;
    private final boolean stream;
    private final GoogleVertexAiChatCompletionTaskSettings taskSettings;

    public GoogleModelGardenAnthropicChatCompletionRequestEntity(
        UnifiedChatInput unifiedChatInput,
        GoogleVertexAiChatCompletionTaskSettings taskSettings
    ) {
        this(
            Objects.requireNonNull(unifiedChatInput).getRequest(),
            Objects.requireNonNull(unifiedChatInput).stream(),
            Objects.requireNonNull(taskSettings)
        );
    }

    public GoogleModelGardenAnthropicChatCompletionRequestEntity(
        UnifiedCompletionRequest unifiedRequest,
        boolean stream,
        GoogleVertexAiChatCompletionTaskSettings taskSettings
    ) {
        this.unifiedRequest = Objects.requireNonNull(unifiedRequest);
        this.stream = stream;
        this.taskSettings = taskSettings;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ANTHROPIC_VERSION, VERTEX_2023_10_16);
        builder.field(MESSAGES_FIELD, unifiedRequest.messages());
        if (unifiedRequest.temperature() != null) {
            builder.field(TEMPERATURE_FIELD, unifiedRequest.temperature());
        }
        var toolChoice = unifiedRequest.toolChoice();
        if (toolChoice != null) {
            if (toolChoice instanceof UnifiedCompletionRequest.ToolChoiceObject) {
                builder.startObject(TOOL_CHOICE_FIELD);
                builder.field(TYPE_FIELD, ((UnifiedCompletionRequest.ToolChoiceObject) toolChoice).type());
                builder.endObject();
            } else if (toolChoice instanceof UnifiedCompletionRequest.ToolChoiceString) {
                throw new ElasticsearchStatusException(
                    "Tool choice value is not supported as string by Google Model Garden Anthropic Chat Completion.",
                    RestStatus.BAD_REQUEST
                );
            }
        }
        var tools = unifiedRequest.tools();
        if (tools != null && (tools.isEmpty() == false)) {
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
        if (unifiedRequest.topP() != null) {
            builder.field(TOP_P_FIELD, unifiedRequest.topP());
        }
        builder.field(STREAM_FIELD, stream);
        var maxTokens = Objects.requireNonNullElse(
            unifiedRequest.maxCompletionTokens(),
            Objects.requireNonNullElse(taskSettings.maxTokens(), DEFAULT_MAX_TOKENS)
        );
        builder.field(MAX_TOKENS_FIELD, maxTokens);
        builder.endObject();
        return builder;
    }
}
