/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.request.completion;

import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.anthropic.request.AnthropicToolUtils;
import org.elasticsearch.xpack.inference.services.googlevertexai.completion.GoogleVertexAiChatCompletionTaskSettings;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.MESSAGES_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.TEMPERATURE_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.TOP_P_FIELD;
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
        AnthropicToolUtils.writeToolChoice(builder, unifiedRequest.toolChoice());
        AnthropicToolUtils.writeTools(builder, unifiedRequest.tools());
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
