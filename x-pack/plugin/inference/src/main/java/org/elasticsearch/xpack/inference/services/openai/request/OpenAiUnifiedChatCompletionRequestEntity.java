/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.request;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.inference.completion.Reasoning;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.openai.completion.OpenAiChatCompletionModel;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.MAX_COMPLETION_TOKENS_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.MESSAGES_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.MODEL_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.STOP_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.TEMPERATURE_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.TOOL_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.TOP_P_FIELD;

/**
 * Serializes a unified chat completion request for OpenAI's Chat Completions API.
 * <p>
 * OpenAI Chat Completions does not accept the nested {@code reasoning} object from the unified API.
 * This entity serializes the request independently of {@code UnifiedChatCompletionRequestEntity} so
 * that {@code reasoning.effort} can be mapped to a top-level {@code reasoning_effort} string.
 * Fields such as {@code summary}, {@code exclude}, and {@code enabled} are accepted by the unified
 * API for forward compatibility but are not forwarded on this path.
 * Unified {@code cache_control} and {@code session_id} are also omitted: OpenAI Chat Completions
 * does not accept those fields (prompt caching uses a different shape), and sticky sessions are
 * an Elastic Inference Service feature.
 */
public class OpenAiUnifiedChatCompletionRequestEntity implements ToXContentObject {

    public static final String USER_FIELD = "user";
    public static final String REASONING_EFFORT_FIELD = "reasoning_effort";

    private static final String STREAM_FIELD = "stream";
    private static final String NUMBER_OF_RETURNED_CHOICES_FIELD = "n";
    private static final String STREAM_OPTIONS_FIELD = "stream_options";
    private static final String INCLUDE_USAGE_FIELD = "include_usage";

    private final OpenAiChatCompletionModel model;
    private final UnifiedCompletionRequest unifiedRequest;
    private final boolean stream;

    public OpenAiUnifiedChatCompletionRequestEntity(UnifiedChatInput unifiedChatInput, OpenAiChatCompletionModel model) {
        Objects.requireNonNull(unifiedChatInput);
        this.model = Objects.requireNonNull(model);
        this.unifiedRequest = unifiedChatInput.getRequest();
        this.stream = unifiedChatInput.stream();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.field(MESSAGES_FIELD, unifiedRequest.messages());
        if (unifiedRequest.stop() != null && unifiedRequest.stop().isEmpty() == false) {
            builder.field(STOP_FIELD, unifiedRequest.stop());
        }
        if (unifiedRequest.temperature() != null) {
            builder.field(TEMPERATURE_FIELD, unifiedRequest.temperature());
        }
        if (unifiedRequest.toolChoice() != null) {
            unifiedRequest.toolChoice().toXContent(builder, params);
        }
        if (unifiedRequest.tools() != null && unifiedRequest.tools().isEmpty() == false) {
            builder.field(TOOL_FIELD, unifiedRequest.tools());
        }
        if (unifiedRequest.topP() != null) {
            builder.field(TOP_P_FIELD, unifiedRequest.topP());
        }
        if (unifiedRequest.maxCompletionTokens() != null) {
            builder.field(MAX_COMPLETION_TOKENS_FIELD, unifiedRequest.maxCompletionTokens());
        }
        builder.field(MODEL_FIELD, model.getServiceSettings().modelId());

        // Underlying providers expect OpenAI to only return 1 possible choice.
        builder.field(NUMBER_OF_RETURNED_CHOICES_FIELD, 1);
        builder.field(STREAM_FIELD, stream);
        if (stream) {
            builder.startObject(STREAM_OPTIONS_FIELD);
            builder.field(INCLUDE_USAGE_FIELD, true);
            builder.endObject();
        }

        if (Strings.isNullOrEmpty(model.getTaskSettings().user()) == false) {
            builder.field(USER_FIELD, model.getTaskSettings().user());
        }

        if (unifiedRequest.reasoning() != null) {
            writeReasoningEffort(builder, unifiedRequest.reasoning());
        }

        builder.endObject();
        return builder;
    }

    /**
     * Maps unified {@code reasoning.effort} to OpenAI Chat Completions {@code reasoning_effort}.
     */
    static void writeReasoningEffort(XContentBuilder builder, Reasoning reasoning) throws IOException {
        if (reasoning.effort() == null) {
            throw new ElasticsearchStatusException(
                "OpenAI chat completion requires [reasoning.effort] to map to [" + REASONING_EFFORT_FIELD + "]",
                RestStatus.BAD_REQUEST
            );
        }
        builder.field(REASONING_EFFORT_FIELD, reasoning.effort().toString());
    }
}
