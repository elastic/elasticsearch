/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.fireworksai.request;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Request entity for FireworksAI embeddings API.
 * Supports OpenAI-compatible embeddings endpoint with optional dimensions parameter.
 */
public record FireworksAiEmbeddingsRequestEntity(List<String> input, String model, @Nullable Integer dimensions)
    implements
        ToXContentObject {

    private static final String INPUT_FIELD = "input";
    private static final String MODEL_FIELD = "model";
    private static final String DIMENSIONS_FIELD = "dimensions";

    public FireworksAiEmbeddingsRequestEntity {
        Objects.requireNonNull(input);
        Objects.requireNonNull(model);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(INPUT_FIELD, input);
        builder.field(MODEL_FIELD, model);

        if (dimensions != null) {
            builder.field(DIMENSIONS_FIELD, dimensions);
        }

        builder.endObject();
        return builder;
    }
}
