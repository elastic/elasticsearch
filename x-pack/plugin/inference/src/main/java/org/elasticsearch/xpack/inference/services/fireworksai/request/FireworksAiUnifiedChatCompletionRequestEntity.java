/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.fireworksai.request;

import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.external.unified.UnifiedChatCompletionRequestEntity;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.fireworksai.completion.FireworksAiChatCompletionModel;

import java.io.IOException;
import java.util.Objects;

class FireworksAiUnifiedChatCompletionRequestEntity implements ToXContentObject {

    private final UnifiedChatCompletionRequestEntity unifiedRequestEntity;
    private final FireworksAiChatCompletionModel model;

    FireworksAiUnifiedChatCompletionRequestEntity(UnifiedChatInput unifiedChatInput, FireworksAiChatCompletionModel model) {
        this.unifiedRequestEntity = new UnifiedChatCompletionRequestEntity(unifiedChatInput);
        this.model = Objects.requireNonNull(model);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        unifiedRequestEntity.toXContent(
            builder,
            UnifiedCompletionRequest.withMaxCompletionTokens(model.getServiceSettings().modelId(), params)
        );

        if (Strings.isNullOrEmpty(model.getTaskSettings().user()) == false) {
            builder.field(ServiceFields.USER, model.getTaskSettings().user());
        }

        builder.endObject();
        return builder;
    }
}
