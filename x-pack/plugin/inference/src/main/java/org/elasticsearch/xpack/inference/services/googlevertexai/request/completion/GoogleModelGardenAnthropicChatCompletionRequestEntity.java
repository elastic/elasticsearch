/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.request.completion;

import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.mistral.MistralConstants.MAX_TOKENS_FIELD;

public class GoogleModelGardenAnthropicChatCompletionRequestEntity implements ToXContentObject {

    private static final String ANTHROPIC_VERSION = "anthropic_version";
    private static final String VERTEX_2023_10_16 = "vertex-2023-10-16";
    private static final String STREAM_FIELD = "stream";
    private static final long DEFAULT_MAX_TOKENS_VALUE = 10L;

    private final UnifiedCompletionRequest unifiedRequest;
    private final boolean stream;

    public GoogleModelGardenAnthropicChatCompletionRequestEntity(UnifiedChatInput unifiedChatInput) {
        this(Objects.requireNonNull(unifiedChatInput).getRequest(), Objects.requireNonNull(unifiedChatInput).stream());
    }

    public GoogleModelGardenAnthropicChatCompletionRequestEntity(UnifiedCompletionRequest unifiedRequest, boolean stream) {
        this.unifiedRequest = Objects.requireNonNull(unifiedRequest);
        this.stream = stream;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ANTHROPIC_VERSION, VERTEX_2023_10_16);
        unifiedRequest.toXContent(builder, UnifiedCompletionRequest.withMaxTokens(params));

        builder.field(STREAM_FIELD, stream);

        if (unifiedRequest.maxCompletionTokens() == null) {
            builder.field(MAX_TOKENS_FIELD, DEFAULT_MAX_TOKENS_VALUE);
        }

        builder.endObject();

        return builder;
    }
}
