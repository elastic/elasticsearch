/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.nvidia.request.embeddings;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.cohere.CohereServiceFields;
import org.elasticsearch.xpack.inference.services.cohere.CohereTruncation;
import org.elasticsearch.xpack.inference.services.nvidia.NvidiaUtils;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * NvidiaEmbeddingsRequestEntity is responsible for creating the request entity for Nvidia embeddings.
 * It implements {@link ToXContentObject} to allow serialization to XContent format.
 */
public record NvidiaEmbeddingsRequestEntity(
    List<String> input,
    String modelId,
    @Nullable InputType inputType,
    @Nullable CohereTruncation truncation
) implements ToXContentObject {

    private static final String INPUT_FIELD = "input";
    private static final String MODEL_FIELD = "model";
    private static final String INPUT_TYPE_FIELD = "input_type";

    public NvidiaEmbeddingsRequestEntity {
        Objects.requireNonNull(input);
        Objects.requireNonNull(modelId);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(INPUT_FIELD, input);
        builder.field(MODEL_FIELD, modelId);
        var inputTypeToUse = NvidiaUtils.inputTypeToString(inputType);
        if (inputTypeToUse != null) {
            builder.field(INPUT_TYPE_FIELD, inputTypeToUse);
        }
        if (truncation != null) {
            builder.field(CohereServiceFields.TRUNCATE, truncation);
        }
        builder.endObject();
        return builder;
    }
}
