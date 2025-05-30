/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface.request.completion;

import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.external.unified.UnifiedChatCompletionRequestEntity;
import org.elasticsearch.xpack.inference.services.huggingface.completion.HuggingFaceChatCompletionModel;

import java.io.IOException;
import java.util.Objects;

public class HuggingFaceUnifiedChatCompletionRequestEntity implements ToXContentObject {

    private final HuggingFaceChatCompletionModel model;
    private final UnifiedChatCompletionRequestEntity unifiedRequestEntity;

    public HuggingFaceUnifiedChatCompletionRequestEntity(UnifiedChatInput unifiedChatInput, HuggingFaceChatCompletionModel model) {
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
