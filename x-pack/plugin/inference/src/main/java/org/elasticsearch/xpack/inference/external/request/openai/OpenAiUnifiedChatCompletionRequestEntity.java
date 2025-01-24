/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.openai;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.external.unified.UnifiedChatCompletionRequestEntity;
import org.elasticsearch.xpack.inference.services.openai.completion.OpenAiChatCompletionModel;

import java.io.IOException;
import java.util.Objects;

public class OpenAiUnifiedChatCompletionRequestEntity implements ToXContentObject {

    public static final String USER_FIELD = "user";
    private static final String MODEL_FIELD = "model";

    private final OpenAiChatCompletionModel model;
    private final UnifiedChatCompletionRequestEntity unifiedRequestEntity;

    public OpenAiUnifiedChatCompletionRequestEntity(UnifiedChatInput unifiedChatInput, OpenAiChatCompletionModel model) {
        this.unifiedRequestEntity = new UnifiedChatCompletionRequestEntity(Objects.requireNonNull(unifiedChatInput));
        this.model = Objects.requireNonNull(model);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        unifiedRequestEntity.toXContent(builder, params);

        builder.field(MODEL_FIELD, model.getServiceSettings().modelId());

        if (Strings.isNullOrEmpty(model.getTaskSettings().user()) == false) {
            builder.field(USER_FIELD, model.getTaskSettings().user());
        }

        builder.endObject();

        return builder;
    }
}
