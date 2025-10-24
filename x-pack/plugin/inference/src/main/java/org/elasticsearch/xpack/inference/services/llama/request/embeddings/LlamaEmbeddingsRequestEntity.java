/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.llama.request.embeddings;

import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * LlamaEmbeddingsRequestEntity is responsible for creating the request entity for Llama embeddings.
 * It implements ToXContentObject to allow serialization to XContent format.
 */
public record LlamaEmbeddingsRequestEntity(String modelId, List<String> contents) implements ToXContentObject {

    public static final String CONTENTS_FIELD = "contents";
    public static final String MODEL_ID_FIELD = "model_id";

    /**
     * Constructs a LlamaEmbeddingsRequestEntity with the specified model ID and contents.
     *
     * @param modelId  the ID of the model to use for embeddings
     * @param contents the list of contents to generate embeddings for
     */
    public LlamaEmbeddingsRequestEntity {
        Objects.requireNonNull(modelId);
        Objects.requireNonNull(contents);
    }

    /**
     * Constructs a LlamaEmbeddingsRequestEntity with the specified model ID and a single content string.
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.field(MODEL_ID_FIELD, modelId);
        builder.field(CONTENTS_FIELD, contents);

        builder.endObject();

        return builder;
    }
}
