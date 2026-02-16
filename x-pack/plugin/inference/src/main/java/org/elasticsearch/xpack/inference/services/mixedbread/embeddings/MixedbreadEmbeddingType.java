/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread.embeddings;

import org.elasticsearch.common.Strings;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Map;

/**
 * Defines the type of embedding that the mixedbread api should return for a request.
 */
public enum MixedbreadEmbeddingType {
    /**
     * Use this when you want to get back the default float embeddings. Valid for all models.
     */
    FLOAT(DenseVectorFieldMapper.ElementType.FLOAT, MixedbreadEmbeddingType.RequestConstants.FLOAT),
    /**
     * Use this when you want to get back signed int8 embeddings.
     */
    INT8(DenseVectorFieldMapper.ElementType.BYTE, MixedbreadEmbeddingType.RequestConstants.INT8),
    /**
     * This is a synonym for INT8
     */
    BYTE(DenseVectorFieldMapper.ElementType.BYTE, MixedbreadEmbeddingType.RequestConstants.INT8);

    private static final class RequestConstants {
        private static final String FLOAT = "float";
        private static final String INT8 = "int8";
    }

    private static final Map<DenseVectorFieldMapper.ElementType, MixedbreadEmbeddingType> ELEMENT_TYPE_TO_MIXEDBREAD_EMBEDDING = Map.of(
        DenseVectorFieldMapper.ElementType.FLOAT,
        FLOAT,
        DenseVectorFieldMapper.ElementType.BYTE,
        BYTE
    );
    static final EnumSet<DenseVectorFieldMapper.ElementType> SUPPORTED_ELEMENT_TYPES = EnumSet.copyOf(
        ELEMENT_TYPE_TO_MIXEDBREAD_EMBEDDING.keySet()
    );

    private final DenseVectorFieldMapper.ElementType elementType;
    private final String requestString;

    MixedbreadEmbeddingType(DenseVectorFieldMapper.ElementType elementType, String requestString) {
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

    public static String toLowerCase(MixedbreadEmbeddingType type) {
        return type.toString().toLowerCase(Locale.ROOT);
    }

    public static MixedbreadEmbeddingType fromString(String name) {
        return valueOf(name.trim().toUpperCase(Locale.ROOT));
    }

    public static MixedbreadEmbeddingType fromElementType(DenseVectorFieldMapper.ElementType elementType) {
        var embedding = ELEMENT_TYPE_TO_MIXEDBREAD_EMBEDDING.get(elementType);

        if (embedding == null) {
            var validElementTypes = SUPPORTED_ELEMENT_TYPES.stream()
                .map(value -> value.toString().toLowerCase(Locale.ROOT))
                .toArray(String[]::new);
            Arrays.sort(validElementTypes);

            throw new IllegalArgumentException(
                Strings.format(
                    "Element type [%s] does not map to a Mixedbread embedding value, must be one of [%s]",
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
