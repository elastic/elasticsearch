/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.elastic;

import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.external.unified.UnifiedChatCompletionRequestEntity;

import java.io.IOException;
import java.util.Objects;

public class ElasticInferenceServiceUnifiedChatCompletionRequestEntity implements ToXContentObject {
    private static final String MODEL_FIELD = "model";

    private final UnifiedChatCompletionRequestEntity unifiedRequestEntity;
    private final String modelId;

    public ElasticInferenceServiceUnifiedChatCompletionRequestEntity(UnifiedChatInput unifiedChatInput, String modelId) {
        this.unifiedRequestEntity = new UnifiedChatCompletionRequestEntity(Objects.requireNonNull(unifiedChatInput));
        this.modelId = Objects.requireNonNull(modelId);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        unifiedRequestEntity.toXContent(builder, params);
        builder.field(MODEL_FIELD, modelId);
        builder.endObject();

        return builder;
    }
}
