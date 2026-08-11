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
import org.elasticsearch.xpack.inference.external.unified.UnifiedChatCompletionRequestEntity;
import org.elasticsearch.xpack.inference.services.openai.completion.OpenAiChatCompletionModel;

import java.io.IOException;
import java.util.Objects;

/**
 * Serializes a unified chat completion request for OpenAI's Chat Completions API.
 * <p>
 * The unified Inference API uses a nested {@code reasoning} object. OpenAI Chat Completions
 * expects a top-level {@code reasoning_effort} string instead, so this entity strips the nested
 * object and maps {@code reasoning.effort} to {@code reasoning_effort}. Fields such as
 * {@code summary}, {@code exclude}, and {@code enabled} are accepted by the unified API for
 * forward compatibility but are not forwarded on this path.
 */
public class OpenAiUnifiedChatCompletionRequestEntity implements ToXContentObject {

    public static final String USER_FIELD = "user";
    public static final String REASONING_EFFORT_FIELD = "reasoning_effort";

    private final OpenAiChatCompletionModel model;
    private final UnifiedChatCompletionRequestEntity unifiedRequestEntity;
    private final Reasoning reasoning;

    public OpenAiUnifiedChatCompletionRequestEntity(UnifiedChatInput unifiedChatInput, OpenAiChatCompletionModel model) {
        Objects.requireNonNull(unifiedChatInput);
        this.model = Objects.requireNonNull(model);
        this.reasoning = unifiedChatInput.getRequest().reasoning();
        // OpenAI Chat Completions does not accept the nested reasoning object from the unified API.
        this.unifiedRequestEntity = new UnifiedChatCompletionRequestEntity(
            new UnifiedChatInput(withoutReasoning(unifiedChatInput.getRequest()), unifiedChatInput.stream())
        );
    }

    private static UnifiedCompletionRequest withoutReasoning(UnifiedCompletionRequest request) {
        if (request.reasoning() == null) {
            return request;
        }
        return new UnifiedCompletionRequest(
            request.messages(),
            request.model(),
            request.maxCompletionTokens(),
            request.stop(),
            request.temperature(),
            request.toolChoice(),
            request.tools(),
            request.topP(),
            null,
            request.cacheControl(),
            request.sessionId()
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        unifiedRequestEntity.toXContent(
            builder,
            UnifiedCompletionRequest.withMaxCompletionTokens(model.getServiceSettings().modelId(), params)
        );

        if (Strings.isNullOrEmpty(model.getTaskSettings().user()) == false) {
            builder.field(USER_FIELD, model.getTaskSettings().user());
        }

        if (reasoning != null) {
            writeReasoningEffort(builder, reasoning);
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
