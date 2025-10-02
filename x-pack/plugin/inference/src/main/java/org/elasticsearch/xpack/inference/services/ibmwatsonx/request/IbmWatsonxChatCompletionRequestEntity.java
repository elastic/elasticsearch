/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx.request;

import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.external.unified.UnifiedChatCompletionRequestEntity;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.completion.IbmWatsonxChatCompletionModel;

import java.io.IOException;
import java.util.Objects;

/**
 * IbmWatsonxChatCompletionRequestEntity is responsible for creating the request entity for Watsonx chat completion.
 * It implements ToXContentObject to allow serialization to XContent format.
 */
public class IbmWatsonxChatCompletionRequestEntity implements ToXContentObject {

    private final IbmWatsonxChatCompletionModel model;
    private final UnifiedChatCompletionRequestEntity unifiedRequestEntity;

    private static final String PROJECT_ID_FIELD = "project_id";

    public IbmWatsonxChatCompletionRequestEntity(UnifiedChatInput unifiedChatInput, IbmWatsonxChatCompletionModel model) {
        this.unifiedRequestEntity = new UnifiedChatCompletionRequestEntity(unifiedChatInput);
        this.model = Objects.requireNonNull(model);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(PROJECT_ID_FIELD, model.getServiceSettings().projectId());
        unifiedRequestEntity.toXContent(
            builder,
            UnifiedCompletionRequest.withMaxTokensAndSkipStreamOptionsField(model.getServiceSettings().modelId(), params)
        );
        builder.endObject();
        return builder;
    }
}
