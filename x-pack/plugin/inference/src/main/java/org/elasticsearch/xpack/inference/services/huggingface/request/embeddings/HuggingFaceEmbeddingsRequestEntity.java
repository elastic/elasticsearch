/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface.request.embeddings;

import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * This class represents the request entity for Hugging Face embeddings.
 * It contains a list of input strings that will be used to generate embeddings.
 */
public record HuggingFaceEmbeddingsRequestEntity(List<String> inputs) implements ToXContentObject {

    private static final String INPUTS_FIELD = "inputs";

    public HuggingFaceEmbeddingsRequestEntity {
        Objects.requireNonNull(inputs);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.field(INPUTS_FIELD, inputs);

        builder.endObject();
        return builder;
    }
}
