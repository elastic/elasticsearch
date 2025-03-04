/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai.embeddings;

import org.elasticsearch.common.Strings;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Map;

/**
 * Defines the type of embedding that the VoyageAI api should return for a request.
 *
 * <p>
 * <a href="https://docs.voyageai.com/reference/embeddings-api">See api docs for details.</a>
 * </p>
 */
public enum VoyageAIEmbeddingType {
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
    BIT(DenseVectorFieldMapper.ElementType.BIT, RequestConstants.BINARY),
    /**
     * This is a synonym for BIT
     */
    BINARY(DenseVectorFieldMapper.ElementType.BIT, RequestConstants.BINARY);

    private static final class RequestConstants {
        private static final String FLOAT = "float";
        private static final String INT8 = "int8";
        private static final String BINARY = "binary";
    }

    private static final Map<DenseVectorFieldMapper.ElementType, VoyageAIEmbeddingType> ELEMENT_TYPE_TO_VOYAGE_EMBEDDING = Map.of(
        DenseVectorFieldMapper.ElementType.FLOAT,
        FLOAT,
        DenseVectorFieldMapper.ElementType.BYTE,
        BYTE,
        DenseVectorFieldMapper.ElementType.BIT,
        BIT
    );
    static final EnumSet<DenseVectorFieldMapper.ElementType> SUPPORTED_ELEMENT_TYPES = EnumSet.copyOf(
        ELEMENT_TYPE_TO_VOYAGE_EMBEDDING.keySet()
    );

    private final DenseVectorFieldMapper.ElementType elementType;
    private final String requestString;

    VoyageAIEmbeddingType(DenseVectorFieldMapper.ElementType elementType, String requestString) {
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

    public static String toLowerCase(VoyageAIEmbeddingType type) {
        return type.toString().toLowerCase(Locale.ROOT);
    }

    public static VoyageAIEmbeddingType fromString(String name) {
        return valueOf(name.trim().toUpperCase(Locale.ROOT));
    }

    public static VoyageAIEmbeddingType fromElementType(DenseVectorFieldMapper.ElementType elementType) {
        var embedding = ELEMENT_TYPE_TO_VOYAGE_EMBEDDING.get(elementType);

        if (embedding == null) {
            var validElementTypes = SUPPORTED_ELEMENT_TYPES.stream()
                .map(value -> value.toString().toLowerCase(Locale.ROOT))
                .toArray(String[]::new);
            Arrays.sort(validElementTypes);

            throw new IllegalArgumentException(
                Strings.format(
                    "Element type [%s] does not map to a VoyageAI embedding value, must be one of [%s]",
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
}
