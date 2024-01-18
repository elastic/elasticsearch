/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.embeddings;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Locale;

/**
 * Defines the type of embedding that the cohere api should return for a request.
 *
 * <p>
 * <a href="https://docs.cohere.com/reference/embed">See api docs for details.</a>
 * </p>
 */
public enum CohereEmbeddingType implements Writeable {
    /**
     * Use this when you want to get back the default float embeddings. Valid for all models.
     */
    FLOAT,
    /**
     * Use this when you want to get back signed int8 embeddings. Valid for only v3 models.
     */
    INT8;

    public static String NAME = "cohere_embedding_type";

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }

    public static String toLowerCase(CohereEmbeddingType type) {
        return type.toString().toLowerCase(Locale.ROOT);
    }

    public static CohereEmbeddingType fromString(String name) {
        return valueOf(name.trim().toUpperCase(Locale.ROOT));
    }

    public static CohereEmbeddingType fromStream(StreamInput in) throws IOException {
        return in.readOptionalEnum(CohereEmbeddingType.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalEnum(this);
    }
}
