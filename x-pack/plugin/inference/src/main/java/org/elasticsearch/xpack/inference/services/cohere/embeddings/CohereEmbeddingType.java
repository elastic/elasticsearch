/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;

import java.util.Locale;

/**
 * Defines the type of embedding that the cohere api should return for a request.
 *
 * <p>
 * <a href="https://docs.cohere.com/reference/embed">See api docs for details.</a>
 * </p>
 */
public enum CohereEmbeddingType {
    /**
     * Use this when you want to get back the default float embeddings. Valid for all models.
     */
    FLOAT(DenseVectorFieldMapper.ElementType.FLOAT),
    /**
     * Use this when you want to get back signed int8 embeddings. Valid for only v3 models.
     */
    INT8(DenseVectorFieldMapper.ElementType.BYTE),
    /**
     * This is a synonym for INT8
     */
    BYTE(DenseVectorFieldMapper.ElementType.BYTE);

    private final DenseVectorFieldMapper.ElementType elementType;

    CohereEmbeddingType(DenseVectorFieldMapper.ElementType elementType) {
        this.elementType = elementType;
    }

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

    public DenseVectorFieldMapper.ElementType toElementType() {
        return elementType;
    }

    /**
     * Returns an embedding type that is known based on the transport version provided. If the embedding type enum was not yet
     * introduced it will be defaulted INT8.
     *
     * @param embeddingType the value to translate if necessary
     * @param version the version that dictates the translation
     * @return the embedding type that is known to the version passed in
     */
    public static CohereEmbeddingType translateToVersion(CohereEmbeddingType embeddingType, TransportVersion version) {
        if (version.before(TransportVersions.ML_INFERENCE_EMBEDDING_BYTE_ADDED) && embeddingType == BYTE) {
            return INT8;
        }

        return embeddingType;
    }
}
