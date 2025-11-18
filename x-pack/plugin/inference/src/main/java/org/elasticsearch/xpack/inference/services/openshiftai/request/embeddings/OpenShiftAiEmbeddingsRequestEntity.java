/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openshiftai.request.embeddings;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * OpenShiftAiEmbeddingsRequestEntity is responsible for creating the request entity for OpenShift AI embeddings.
 * It implements ToXContentObject to allow serialization to XContent format.
 */
public record OpenShiftAiEmbeddingsRequestEntity(
    List<String> input,
    @Nullable String modelId,
    @Nullable Integer dimensions,
    boolean dimensionsSetByUser
) implements ToXContentObject {

    private static final String INPUT_FIELD = "input";
    private static final String MODEL_FIELD = "model";
    private static final String DIMENSIONS_FIELD = "dimensions";

    public OpenShiftAiEmbeddingsRequestEntity {
        Objects.requireNonNull(input);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(INPUT_FIELD, input);
        if (modelId != null) {
            builder.field(MODEL_FIELD, modelId);
        }
        if (dimensionsSetByUser && dimensions != null) {
            builder.field(DIMENSIONS_FIELD, dimensions);
        }
        builder.endObject();
        return builder;
    }
}
