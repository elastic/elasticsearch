/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.llama.request.completion;

import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.external.unified.UnifiedChatCompletionRequestEntity;
import org.elasticsearch.xpack.inference.services.llama.completion.LlamaChatCompletionModel;

import java.io.IOException;
import java.util.Objects;


/**
 * LlamaChatCompletionRequestEntity is responsible for creating the request entity for Llama chat completion.
 * It implements ToXContentObject to allow serialization to XContent format.
 */
public class LlamaChatCompletionRequestEntity implements ToXContentObject {

    private final LlamaChatCompletionModel model;
    private final UnifiedChatCompletionRequestEntity unifiedRequestEntity;

    /**
     * Constructs a LlamaChatCompletionRequestEntity with the specified unified chat input and model.
     *
     * @param unifiedChatInput the unified chat input containing messages and parameters for the completion request
     * @param model the Llama chat completion model to be used for the request
     */
    public LlamaChatCompletionRequestEntity(UnifiedChatInput unifiedChatInput, LlamaChatCompletionModel model) {
        this.unifiedRequestEntity = new UnifiedChatCompletionRequestEntity(unifiedChatInput);
        this.model = Objects.requireNonNull(model);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        unifiedRequestEntity.toXContent(builder, UnifiedCompletionRequest.withMaxTokens(model.getServiceSettings().modelId(), params));
        builder.endObject();
        return builder;
    }
}
