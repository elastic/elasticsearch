/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Map;

/**
 * Defines the type of embedding that the Jina AI API should return for a request.
 *
 */
public enum JinaAIEmbeddingType {
    /**
     * Use this when you want to get back the default float embeddings.
     */
    FLOAT(DenseVectorFieldMapper.ElementType.FLOAT, RequestConstants.FLOAT),
    /**
     * Use this when you want to get back binary embeddings.
     */
    BIT(DenseVectorFieldMapper.ElementType.BIT, RequestConstants.BIT),
    /**
     * This is a synonym for BIT
     */
    BINARY(DenseVectorFieldMapper.ElementType.BIT, RequestConstants.BIT);

    private static final class RequestConstants {
        private static final String FLOAT = "float";
        private static final String BIT = "binary";
    }

    private static final Map<DenseVectorFieldMapper.ElementType, JinaAIEmbeddingType> ELEMENT_TYPE_TO_JINA_AI_EMBEDDING = Map.of(
        DenseVectorFieldMapper.ElementType.FLOAT,
        FLOAT,
        DenseVectorFieldMapper.ElementType.BIT,
        BIT
    );
    static final EnumSet<DenseVectorFieldMapper.ElementType> SUPPORTED_ELEMENT_TYPES = EnumSet.copyOf(
        ELEMENT_TYPE_TO_JINA_AI_EMBEDDING.keySet()
    );

    private final DenseVectorFieldMapper.ElementType elementType;
    private final String requestString;

    JinaAIEmbeddingType(DenseVectorFieldMapper.ElementType elementType, String requestString) {
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

    public static String toLowerCase(JinaAIEmbeddingType type) {
        return type.toString().toLowerCase(Locale.ROOT);
    }

    public static JinaAIEmbeddingType fromString(String name) {
        return valueOf(name.trim().toUpperCase(Locale.ROOT));
    }

    public static JinaAIEmbeddingType fromElementType(DenseVectorFieldMapper.ElementType elementType) {
        var embedding = ELEMENT_TYPE_TO_JINA_AI_EMBEDDING.get(elementType);

        if (embedding == null) {
            var validElementTypes = SUPPORTED_ELEMENT_TYPES.stream()
                .map(value -> value.toString().toLowerCase(Locale.ROOT))
                .toArray(String[]::new);
            Arrays.sort(validElementTypes);

            throw new IllegalArgumentException(
                Strings.format(
                    "Element type [%s] does not map to a Jina AI embedding value, must be one of [%s]",
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
     * introduced it will be defaulted FLOAT.
     *
     * @param embeddingType the value to translate if necessary
     * @param version the version that dictates the translation
     * @return the embedding type that is known to the version passed in
     */
    public static JinaAIEmbeddingType translateToVersion(JinaAIEmbeddingType embeddingType, TransportVersion version) {
        if (version.onOrAfter(TransportVersions.JINA_AI_EMBEDDING_TYPE_SUPPORT_ADDED)
            || version.isPatchFrom(TransportVersions.JINA_AI_EMBEDDING_TYPE_SUPPORT_ADDED_BACKPORT_8_19)) {
            return embeddingType;
        }

        return FLOAT;
    }
}
