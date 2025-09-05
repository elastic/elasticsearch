/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Map;

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
    FLOAT(DenseVectorFieldMapper.ElementType.FLOAT, RequestConstants.FLOAT),
    /**
     * Use this when you want to get back signed int8 embeddings. Valid for only v3 models.
     */
    INT8(DenseVectorFieldMapper.ElementType.BYTE, RequestConstants.INT8),
    /**
     * This is a synonym for INT8
     */
    BYTE(DenseVectorFieldMapper.ElementType.BYTE, RequestConstants.INT8),
    /**
     * Use this when you want to get back binary embeddings. Valid only for v3 models.
     */
    BIT(DenseVectorFieldMapper.ElementType.BIT, RequestConstants.BIT),
    /**
     * This is a synonym for BIT
     */
    BINARY(DenseVectorFieldMapper.ElementType.BIT, RequestConstants.BIT);

    private static final class RequestConstants {
        private static final String FLOAT = "float";
        private static final String INT8 = "int8";
        private static final String BIT = "binary";
    }

    private static final Map<DenseVectorFieldMapper.ElementType, CohereEmbeddingType> ELEMENT_TYPE_TO_COHERE_EMBEDDING = Map.of(
        DenseVectorFieldMapper.ElementType.FLOAT,
        FLOAT,
        DenseVectorFieldMapper.ElementType.BYTE,
        BYTE,
        DenseVectorFieldMapper.ElementType.BIT,
        BIT
    );
    static final EnumSet<DenseVectorFieldMapper.ElementType> SUPPORTED_ELEMENT_TYPES = EnumSet.copyOf(
        ELEMENT_TYPE_TO_COHERE_EMBEDDING.keySet()
    );

    private final DenseVectorFieldMapper.ElementType elementType;
    private final String requestString;

    CohereEmbeddingType(DenseVectorFieldMapper.ElementType elementType, String requestString) {
        this.elementType = elementType;
        this.requestString = requestString;
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }

    public String toRequestString() {
        return requestString;
    }

    public static String toLowerCase(CohereEmbeddingType type) {
        return type.toString().toLowerCase(Locale.ROOT);
    }

    public static CohereEmbeddingType fromString(String name) {
        return valueOf(name.trim().toUpperCase(Locale.ROOT));
    }

    public static CohereEmbeddingType fromElementType(DenseVectorFieldMapper.ElementType elementType) {
        var embedding = ELEMENT_TYPE_TO_COHERE_EMBEDDING.get(elementType);

        if (embedding == null) {
            var validElementTypes = SUPPORTED_ELEMENT_TYPES.stream()
                .map(value -> value.toString().toLowerCase(Locale.ROOT))
                .toArray(String[]::new);
            Arrays.sort(validElementTypes);

            throw new IllegalArgumentException(
                Strings.format(
                    "Element type [%s] does not map to a Cohere embedding value, must be one of [%s]",
                    elementType,
                    String.join(", ", validElementTypes)
                )
            );
        }

        return embedding;
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
        if (version.before(TransportVersions.V_8_14_0) && embeddingType == BYTE) {
            return INT8;
        }

        if (embeddingType == BIT) {
            if (version.onOrAfter(TransportVersions.COHERE_BIT_EMBEDDING_TYPE_SUPPORT_ADDED)
                || version.isPatchFrom(TransportVersions.COHERE_BIT_EMBEDDING_TYPE_SUPPORT_ADDED_BACKPORT_8_X)) {
                // BIT embedding type is supported in these versions
                return embeddingType;
            } else {
                // BIT embedding type is not supported in these versions
                return INT8;
            }
        }

        return embeddingType;
    }
}
