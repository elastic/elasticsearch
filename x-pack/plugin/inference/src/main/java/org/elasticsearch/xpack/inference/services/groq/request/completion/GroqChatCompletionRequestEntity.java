/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.groq.request.completion;

import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.external.unified.UnifiedChatCompletionRequestEntity;

import java.io.IOException;
import java.util.Objects;

/**
 * GroqChatCompletionRequestEntity is responsible for creating the request entity for Groq chat completion.
 * It implements {@link ToXContentObject} to allow serialization to XContent format.
 */
public class GroqChatCompletionRequestEntity implements ToXContentObject {

    private final String modelId;
    private final UnifiedChatCompletionRequestEntity unifiedRequestEntity;

    public GroqChatCompletionRequestEntity(UnifiedChatInput unifiedChatInput, String modelId) {
        this.unifiedRequestEntity = new UnifiedChatCompletionRequestEntity(unifiedChatInput);
        this.modelId = Objects.requireNonNull(modelId);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        unifiedRequestEntity.toXContent(builder, UnifiedCompletionRequest.withMaxCompletionTokens(modelId, params));
        builder.endObject();
        return builder;
    }
}
