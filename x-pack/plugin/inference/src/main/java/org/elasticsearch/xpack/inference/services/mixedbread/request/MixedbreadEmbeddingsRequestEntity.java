/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread.request;

import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.mistral.MistralConstants.ENCODING_FORMAT_FIELD;

/**
 * MixedbreadEmbeddingsRequestEntity is responsible for creating the request entity for Mixedbread embeddings.
 * It implements ToXContentObject to allow serialization to XContent format.
 */
public record MixedbreadEmbeddingsRequestEntity(String model, List<String> input) implements ToXContentObject {

    public static final String INPUT_FIELD = "input";
    public static final String MODEL_FIELD = "model";

    /**
     * Constructs a MixedbreadEmbeddingsRequestEntity with the specified model ID and input.
     *
     * @param model  the ID of the model to use for embeddings
     * @param input the list of input to generate embeddings for
     */
    public MixedbreadEmbeddingsRequestEntity {
        Objects.requireNonNull(model);
        Objects.requireNonNull(input);
    }

    /**
     * Constructs a MixedbreadEmbeddingsRequestEntity with the specified model ID and a single content string.
     */
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
