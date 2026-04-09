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
import org.elasticsearch.xpack.inference.common.model.Truncation;
import org.elasticsearch.xpack.inference.services.nvidia.NvidiaUtils;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.nvidia.request.NvidiaRequestFields.INPUT_FIELD_NAME;
import static org.elasticsearch.xpack.inference.services.nvidia.request.NvidiaRequestFields.INPUT_TYPE_FIELD_NAME;
import static org.elasticsearch.xpack.inference.services.nvidia.request.NvidiaRequestFields.MODEL_FIELD_NAME;
import static org.elasticsearch.xpack.inference.services.nvidia.request.NvidiaRequestFields.TRUNCATE_FIELD_NAME;

/**
 * NvidiaEmbeddingsRequestEntity is responsible for creating the request entity for Nvidia embeddings.
 * It implements {@link ToXContentObject} to allow serialization to XContent format.
 */
public record NvidiaEmbeddingsRequestEntity(List<String> input, String modelId, InputType inputType, @Nullable Truncation truncation)
    implements
        ToXContentObject {

    public NvidiaEmbeddingsRequestEntity {
        Objects.requireNonNull(input);
        Objects.requireNonNull(modelId);
        Objects.requireNonNull(inputType);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(INPUT_FIELD_NAME, input);
        builder.field(MODEL_FIELD_NAME, modelId);
        builder.field(INPUT_TYPE_FIELD_NAME, NvidiaUtils.inputTypeToString(inputType));
        if (truncation != null) {
            builder.field(TRUNCATE_FIELD_NAME, truncation);
        }
        builder.endObject();
        return builder;
    }
}
