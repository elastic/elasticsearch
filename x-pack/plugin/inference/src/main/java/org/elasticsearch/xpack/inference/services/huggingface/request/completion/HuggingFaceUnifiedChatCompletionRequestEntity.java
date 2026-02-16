/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface.request.completion;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.external.unified.UnifiedChatCompletionRequestEntity;

import java.io.IOException;

/**
 * HuggingFaceUnifiedChatCompletionRequestEntity is responsible for creating the request entity for Hugging Face chat completion.
 * It implements ToXContentObject to allow serialization to XContent format.
 */
public class HuggingFaceUnifiedChatCompletionRequestEntity implements ToXContentObject {

    private final String modelId;
    private final UnifiedChatCompletionRequestEntity unifiedRequestEntity;

    /**
     * Constructs a HuggingFaceUnifiedChatCompletionRequestEntity with the specified unified chat input and model ID.
     *
     * @param unifiedChatInput the unified chat input containing messages and parameters for the completion request
     * @param modelId the Hugging Face chat completion model ID to be used for the request
     */
    public HuggingFaceUnifiedChatCompletionRequestEntity(UnifiedChatInput unifiedChatInput, @Nullable String modelId) {
        this.unifiedRequestEntity = new UnifiedChatCompletionRequestEntity(unifiedChatInput);
        this.modelId = modelId;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        unifiedRequestEntity.toXContent(builder, UnifiedCompletionRequest.withMaxTokens(modelId, params));
        builder.endObject();

        return builder;
    }
}
