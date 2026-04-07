/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.request;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.external.unified.UnifiedChatCompletionRequestEntity;

import java.io.IOException;

public class AzureOpenAiChatCompletionRequestEntity implements ToXContentObject {

    public static final String USER_FIELD = "user";
    private final UnifiedChatCompletionRequestEntity requestEntity;
    private final String user;

    public AzureOpenAiChatCompletionRequestEntity(UnifiedChatInput chatInput, @Nullable String user) {
        this.requestEntity = new UnifiedChatCompletionRequestEntity(chatInput);
        this.user = user;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        requestEntity.toXContent(builder, UnifiedCompletionRequest.withMaxCompletionTokens(params));

        if (Strings.isNullOrEmpty(user) == false) {
            builder.field(USER_FIELD, user);
        }
        builder.endObject();
        return builder;
    }
}
