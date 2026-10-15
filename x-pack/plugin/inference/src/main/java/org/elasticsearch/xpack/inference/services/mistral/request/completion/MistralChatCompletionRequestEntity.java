/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mistral.request.completion;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.UnifiedCompletionRequestBody;
import org.elasticsearch.inference.completion.Content;
import org.elasticsearch.inference.completion.ContentObjects;
import org.elasticsearch.inference.completion.ContentString;
import org.elasticsearch.inference.completion.Message;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;

import java.io.IOException;

import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.CONTENT_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.MAX_TOKENS_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.MESSAGES_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.MODEL_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.ROLE_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.STOP_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.TEMPERATURE_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.TEXT_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.TOOL_CALLS_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.TOOL_CALL_ID_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.TOOL_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.TOP_P_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.TYPE_FIELD;
import static org.elasticsearch.xpack.inference.services.mistral.MistralConstants.REASONING_EFFORT_FIELD;

/**
 * MistralChatCompletionRequestEntity is responsible for creating the request entity for Mistral chat completion.
 * It implements ToXContentObject to allow serialization to XContent format.
 *
 * <p>The body is written explicitly, rather than through the shared unified serialization, so that the request can be
 * adapted to what the Mistral API accepts:
 * <ul>
 *   <li>{@code reasoning_effort} is sent from the request's {@code reasoning.effort}, falling back to the endpoint's
 *       service setting. The value is passed through untouched: Mistral validates which efforts a model supports.</li>
 *   <li>An assistant message that carries {@code reasoning} is sent with its thinking replayed as a thinking chunk
 *       in the {@code content} list, which Mistral requires for multi-turn conversations with reasoning on.</li>
 *   <li>{@code temperature: 0} is sent with {@code top_p: 1} unless the caller chose a {@code top_p}, because Mistral
 *       rejects greedy sampling with any other value.</li>
 *   <li>The {@code developer} role is sent as {@code system}.</li>
 *   <li>The nested unified {@code reasoning} object, {@code cache_control} and {@code session_id} are never forwarded,
 *       and neither is a top-level {@code user} field.</li>
 * </ul>
 */
public class MistralChatCompletionRequestEntity implements ToXContentObject {

    private static final String DEVELOPER_ROLE = "developer";
    private static final String SYSTEM_ROLE = "system";
    private static final String ASSISTANT_ROLE = "assistant";
    private static final String THINKING_TYPE = "thinking";
    private static final String THINKING_FIELD = "thinking";
    private static final String TEXT_TYPE = "text";
    private static final String STREAM_FIELD = "stream";
    private static final String NUMBER_OF_RETURNED_CHOICES_FIELD = "n";
    private static final Float GREEDY_SAMPLING_TOP_P = 1.0f;

    private final String modelId;
    private final UnifiedChatInput unifiedChatInput;
    @Nullable
    private final String defaultReasoningEffort;

    public MistralChatCompletionRequestEntity(UnifiedChatInput unifiedChatInput, @Nullable String modelId) {
        this(unifiedChatInput, modelId, null);
    }

    public MistralChatCompletionRequestEntity(
        UnifiedChatInput unifiedChatInput,
        @Nullable String modelId,
        @Nullable String defaultReasoningEffort
    ) {
        this.unifiedChatInput = unifiedChatInput;
        this.modelId = modelId;
        this.defaultReasoningEffort = defaultReasoningEffort;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        var request = unifiedChatInput.getRequest();

        builder.startObject();

        builder.startArray(MESSAGES_FIELD);
        for (var message : request.messages()) {
            writeMessage(builder, params, message);
        }
        builder.endArray();

        if (request.stop() != null && request.stop().isEmpty() == false) {
            builder.field(STOP_FIELD, request.stop());
        }
        if (request.temperature() != null) {
            builder.field(TEMPERATURE_FIELD, request.temperature());
        }
        if (request.toolChoice() != null) {
            request.toolChoice().toXContent(builder, params);
        }
        if (request.tools() != null && request.tools().isEmpty() == false) {
            builder.field(TOOL_FIELD, request.tools());
        }
        var topP = topP(request);
        if (topP != null) {
            builder.field(TOP_P_FIELD, topP);
        }
        if (request.maxCompletionTokens() != null) {
            builder.field(MAX_TOKENS_FIELD, request.maxCompletionTokens());
        }
        if (modelId != null) {
            builder.field(MODEL_FIELD, modelId);
        }
        // Underlying providers expect only 1 possible choice.
        builder.field(NUMBER_OF_RETURNED_CHOICES_FIELD, 1);
        builder.field(STREAM_FIELD, unifiedChatInput.stream());

        var effort = reasoningEffort(request);
        if (effort != null) {
            builder.field(REASONING_EFFORT_FIELD, effort);
        }
        builder.endObject();
        return builder;
    }

    private static void writeMessage(XContentBuilder builder, Params params, Message message) throws IOException {
        builder.startObject();

        var reasoning = message.reasoning();
        if (ASSISTANT_ROLE.equals(message.role()) && reasoning != null && reasoning.isEmpty() == false) {
            writeContentWithThinking(builder, params, message.content(), reasoning);
        } else if (message.content() != null) {
            message.content().toXContent(builder, params);
        }

        builder.field(ROLE_FIELD, DEVELOPER_ROLE.equals(message.role()) ? SYSTEM_ROLE : message.role());
        if (message.toolCallId() != null) {
            builder.field(TOOL_CALL_ID_FIELD, message.toolCallId());
        }
        if (message.toolCalls() != null) {
            builder.field(TOOL_CALLS_FIELD, message.toolCalls());
        }
        builder.endObject();
    }

    /**
     * Mistral requires the thinking of a previous assistant turn to be replayed as a thinking chunk in the content list.
     */
    private static void writeContentWithThinking(XContentBuilder builder, Params params, @Nullable Content content, String reasoning)
        throws IOException {
        builder.startArray(CONTENT_FIELD);

        builder.startObject();
        builder.field(TYPE_FIELD, THINKING_TYPE);
        builder.startArray(THINKING_FIELD);
        builder.startObject();
        builder.field(TYPE_FIELD, TEXT_TYPE);
        builder.field(TEXT_FIELD, reasoning);
        builder.endObject();
        builder.endArray();
        builder.endObject();

        if (content instanceof ContentString contentString) {
            builder.startObject();
            builder.field(TYPE_FIELD, TEXT_TYPE);
            builder.field(TEXT_FIELD, contentString.content());
            builder.endObject();
        } else if (content instanceof ContentObjects contentObjects) {
            for (var contentObject : contentObjects.contentObjects()) {
                contentObject.toXContent(builder, params);
            }
        }

        builder.endArray();
    }

    @Nullable
    private String reasoningEffort(UnifiedCompletionRequestBody request) {
        if (request.reasoning() != null && request.reasoning().effort() != null) {
            return request.reasoning().effort().toString();
        }
        return defaultReasoningEffort;
    }

    @Nullable
    private static Float topP(UnifiedCompletionRequestBody request) {
        var temperature = request.temperature();
        if (request.topP() == null && temperature != null && temperature == 0f) {
            return GREEDY_SAMPLING_TOP_P;
        }
        return request.topP();
    }
}
