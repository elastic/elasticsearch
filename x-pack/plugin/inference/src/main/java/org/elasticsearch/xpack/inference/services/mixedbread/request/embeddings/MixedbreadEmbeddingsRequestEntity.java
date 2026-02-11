/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread.request.embeddings;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.mixedbread.MixedbreadUtils;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Entity representing the request body for Mixedbread rerank requests.
 *
 * @param input the input to be truncated
 * @param modelId the model identifier
 * @param dimensions specifies the dimensionality of the generated embedding vector
 * @param prompt the query text used to rank the provided documents by relevance
 * @param normalized specifies whether to normalize the embeddings
 * @param encodingFormat specifies the encoding format of the embeddings
 */
public record MixedbreadEmbeddingsRequestEntity(
    List<String> input,
    String modelId,
    @Nullable Integer dimensions,
    @Nullable String prompt,
    @Nullable Boolean normalized,
    @Nullable String encodingFormat
) implements ToXContentObject {
    public MixedbreadEmbeddingsRequestEntity {
        Objects.requireNonNull(input);
        Objects.requireNonNull(modelId);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(MixedbreadUtils.INPUT_NAME, input);
        builder.field(MixedbreadUtils.MODEL_FIELD, modelId);

        if (dimensions != null) {
            builder.field(MixedbreadUtils.DIMENSIONS_FIELD, dimensions);
        }
        if (prompt != null) {
            builder.field(MixedbreadUtils.PROMPT_FIELD, prompt);
        }

        if (normalized != null) {
            builder.field(MixedbreadUtils.NORMALIZED_FIELD, normalized);
        }

        if (encodingFormat != null) {
            builder.field(MixedbreadUtils.ENCODING_FORMAT_FIELD, encodingFormat);
        }
        builder.endObject();
        return builder;
    }
}
