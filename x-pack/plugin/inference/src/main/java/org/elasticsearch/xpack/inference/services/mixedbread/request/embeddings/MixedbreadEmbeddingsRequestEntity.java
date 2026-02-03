/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread.request.embeddings;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.common.model.Truncation;
import org.elasticsearch.xpack.inference.services.mixedbread.MixedbreadUtils;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Entity representing the request body for Mixedbread rerank requests.
 *
 * @param input the input to be truncated
 * @param modelId the model identifier
 * @param inputType the type of input being processed
 * @param truncation specifies whether input text should be truncated if it exceeds the model's maximum supported length
 */
public record MixedbreadEmbeddingsRequestEntity(List<String> input, String modelId, InputType inputType, @Nullable Truncation truncation)
    implements
        ToXContentObject {
    public MixedbreadEmbeddingsRequestEntity {
        Objects.requireNonNull(input);
        Objects.requireNonNull(modelId);
        Objects.requireNonNull(inputType);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(MixedbreadUtils.INPUT_NAME, input);
        builder.field(MixedbreadUtils.MODEL_FIELD, modelId);
        builder.field(MixedbreadUtils.INPUT_TYPE_FIELD, MixedbreadUtils.inputTypeToString(inputType));
        if (truncation != null) {
            builder.field(MixedbreadUtils.TRUNCATE_FIELD, truncation);
        }
        builder.endObject();
        return builder;
    }
}
