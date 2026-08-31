/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud.request;

import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.external.unified.UnifiedChatCompletionRequestEntity;
import org.elasticsearch.xpack.inference.services.tencentcloud.completion.TencentCloudChatCompletionModel;

import java.io.IOException;
import java.util.Objects;

/**
 * Request body for TencentCloud AI Gateway {@code POST /v1/chat/completions}.
 * <pre>
 *   { "model": "deepseek-v3", "messages": [ ... ] }
 * </pre>
 * The Gateway is fully OpenAI-compatible so the inner fields are produced by {@link UnifiedChatCompletionRequestEntity}.
 */
public record TencentCloudChatCompletionRequestEntity(UnifiedChatInput input, TencentCloudChatCompletionModel model)
    implements
        ToXContentObject {

    public TencentCloudChatCompletionRequestEntity {
        Objects.requireNonNull(input);
        Objects.requireNonNull(model);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        var modelId = Objects.requireNonNullElseGet(input.getRequest().model(), model::model);
        builder.startObject();
        new UnifiedChatCompletionRequestEntity(input).toXContent(builder, UnifiedCompletionRequest.withMaxTokens(modelId, params));
        builder.endObject();
        return builder;
    }
}
