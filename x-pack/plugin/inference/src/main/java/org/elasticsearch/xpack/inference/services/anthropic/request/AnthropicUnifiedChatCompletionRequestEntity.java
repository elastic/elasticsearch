/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.anthropic.request;

import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.inference.completion.ContentString;
import org.elasticsearch.inference.completion.Message;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.anthropic.completion.AnthropicChatCompletionTaskSettings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.MAX_TOKENS_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.MESSAGES_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.MODEL_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.TEMPERATURE_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.TEXT_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.TOP_P_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.TYPE_FIELD;

/**
 * Builds the request body for the Anthropic Messages API
 * (<a href="https://platform.claude.com/docs/en/api/messages/create">POST /v1/messages</a>) when invoked through the unified
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
    private static final String SYSTEM_FIELD = "system";
    private static final String STOP_SEQUENCES_FIELD = "stop_sequences";
    private static final String TOP_K_FIELD = "top_k";
    private static final String TEXT_TYPE = "text";
    private static final String SYSTEM_ROLE = "system";

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

        // Anthropic requires system prompts as a top-level "system" field, not inside the messages array.
        List<Message> systemMessages = new ArrayList<>();
        List<Message> nonSystemMessages = new ArrayList<>();
        unifiedRequest.messages().forEach(m -> {
            if (SYSTEM_ROLE.equals(m.role())) {
                systemMessages.add(m);
            } else {
                nonSystemMessages.add(m);
            }
        });

        if (systemMessages.isEmpty() == false) {
            builder.startArray(SYSTEM_FIELD);
            for (var msg : systemMessages) {
                builder.startObject();
                builder.field(TYPE_FIELD, TEXT_TYPE);
                if (msg.content() instanceof ContentString cs) {
                    builder.field(TEXT_FIELD, cs.content());
                }
                builder.endObject();
            }
            builder.endArray();
        }

        builder.field(MESSAGES_FIELD, nonSystemMessages);

        var stop = unifiedRequest.stop();
        if (stop != null && stop.isEmpty() == false) {
            builder.field(STOP_SEQUENCES_FIELD, stop);
        }

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

        if (taskSettings.topK() != null) {
            builder.field(TOP_K_FIELD, taskSettings.topK());
        }

        AnthropicToolUtils.writeToolChoice(builder, unifiedRequest.toolChoice());
        AnthropicToolUtils.writeTools(builder, unifiedRequest.tools());

        builder.field(STREAM_FIELD, stream);

        final long maxTokens = unifiedRequest.maxCompletionTokens() != null
            ? unifiedRequest.maxCompletionTokens()
            : taskSettings.maxTokens();
        builder.field(MAX_TOKENS_FIELD, maxTokens);

        builder.endObject();
        return builder;
    }
}
