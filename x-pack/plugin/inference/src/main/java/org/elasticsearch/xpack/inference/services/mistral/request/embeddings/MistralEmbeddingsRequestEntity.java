/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mistral.request.embeddings;

import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.mistral.MistralConstants.ENCODING_FORMAT_FIELD;
import static org.elasticsearch.xpack.inference.services.mistral.MistralConstants.INPUT_FIELD;
import static org.elasticsearch.xpack.inference.services.mistral.MistralConstants.MODEL_FIELD;

public record MistralEmbeddingsRequestEntity(String model, List<String> input) implements ToXContentObject {
    public MistralEmbeddingsRequestEntity {
        Objects.requireNonNull(model);
        Objects.requireNonNull(input);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.field(MODEL_FIELD, model);
        builder.field(INPUT_FIELD, input);
        builder.field(ENCODING_FORMAT_FIELD, "float");

        builder.endObject();

        return builder;
    }
}
