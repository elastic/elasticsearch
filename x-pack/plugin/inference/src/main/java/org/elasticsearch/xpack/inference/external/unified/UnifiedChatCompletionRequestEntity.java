/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.unified;

import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.inference.UnifiedCompletionRequest.INCLUDE_STREAM_OPTIONS_PARAM;

/**
 * Represents a unified chat completion request entity.
 * This class is used to convert the unified chat input into a format that can be serialized to XContent.
 */
public class UnifiedChatCompletionRequestEntity implements ToXContentFragment {

    public static final String STREAM_FIELD = "stream";
    private static final String NUMBER_OF_RETURNED_CHOICES_FIELD = "n";
    private static final String STREAM_OPTIONS_FIELD = "stream_options";
    private static final String INCLUDE_USAGE_FIELD = "include_usage";

    private final UnifiedCompletionRequest unifiedRequest;
    private final boolean stream;

    public UnifiedChatCompletionRequestEntity(UnifiedChatInput unifiedChatInput) {
        this(Objects.requireNonNull(unifiedChatInput).getRequest(), Objects.requireNonNull(unifiedChatInput).stream());
    }

    public UnifiedChatCompletionRequestEntity(UnifiedCompletionRequest unifiedRequest, boolean stream) {
        this.unifiedRequest = Objects.requireNonNull(unifiedRequest);
        this.stream = stream;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        unifiedRequest.toXContent(builder, params);

        // Underlying providers expect OpenAI to only return 1 possible choice.
        builder.field(NUMBER_OF_RETURNED_CHOICES_FIELD, 1);

        builder.field(STREAM_FIELD, stream);
        // If request is streamed and skip stream options parameter is not true, include stream options in the request.
        if (stream && params.paramAsBoolean(INCLUDE_STREAM_OPTIONS_PARAM, true)) {
            builder.startObject(STREAM_OPTIONS_FIELD);
            builder.field(INCLUDE_USAGE_FIELD, true);
            builder.endObject();
        }

        return builder;
    }
}
