/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.common.Strings;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;

import java.util.Arrays;
import java.util.Locale;

public enum SimilarityMeasure {
    COSINE(DenseVectorFieldMapper.VectorSimilarity.COSINE),
    DOT_PRODUCT(DenseVectorFieldMapper.VectorSimilarity.DOT_PRODUCT),
    L2_NORM(DenseVectorFieldMapper.VectorSimilarity.L2_NORM),;

    private final DenseVectorFieldMapper.VectorSimilarity vectorSimilarity;

    SimilarityMeasure(DenseVectorFieldMapper.VectorSimilarity vectorSimilarity) {
        this.vectorSimilarity = vectorSimilarity;
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }

    public DenseVectorFieldMapper.VectorSimilarity vectorSimilarity() {
        return vectorSimilarity;
    }

    public static SimilarityMeasure fromString(String name) {
        return valueOf(name.trim().toUpperCase(Locale.ROOT));
    }

    /**
     * Converts a string into a {@link SimilarityMeasure}, throwing an {@link IllegalArgumentException} with a user-facing message
     * listing the accepted values when the string is not recognized. This is intended to be used as the conversion function of
     * {@link org.elasticsearch.xcontent.AbstractObjectParser#declareString(java.util.function.BiConsumer, java.util.function.Function,
     * org.elasticsearch.xcontent.ParseField)}. The default message produced by {@link #fromString(String)} exposes the enum constant
     * names and is not suitable for returning to a user.
     */
    public static SimilarityMeasure parseSimilarity(String value) {
        try {
            return fromString(value);
        } catch (IllegalArgumentException e) {
            var validValuesAsStrings = Arrays.stream(values()).map(SimilarityMeasure::toString).toArray(String[]::new);
            throw new IllegalArgumentException(
                Strings.format("Invalid value [%s]; expected one of %s", value, Arrays.toString(validValuesAsStrings))
            );
        }
    }
}
