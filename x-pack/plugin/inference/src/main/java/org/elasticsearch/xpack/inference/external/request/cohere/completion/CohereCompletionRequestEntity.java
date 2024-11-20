/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.cohere.completion;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public record CohereCompletionRequestEntity(List<String> input, @Nullable String model, boolean stream) implements ToXContentObject {

    private static final String MESSAGE_FIELD = "message";
    private static final String MODEL = "model";
    private static final String STREAM = "stream";

    public CohereCompletionRequestEntity {
        Objects.requireNonNull(input);
        Objects.requireNonNull(input.get(0));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        // we only allow one input for completion, so always get the first one
        builder.field(MESSAGE_FIELD, input.get(0));
        if (model != null) {
            builder.field(MODEL, model);
        }

        if (stream) {
            builder.field(STREAM, true);
        }

        builder.endObject();

        return builder;
    }
}
