/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.vector;

import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * The vector similarity metrics {@link Knn} can be asked to use through its {@code vector_similarity} option when it
 * runs over a runtime expression instead of an indexed {@code dense_vector} field. An indexed field takes its metric
 * from the mapping, so the option is rejected there.
 * <p>
 * The constants are named to match {@link DenseVectorFieldMapper.VectorSimilarity} and apply the same score
 * normalization, so a user who knows how their indexed fields score can expect the same behavior here. Each
 * constant pairs the raw similarity computation with the normalization that turns that raw value into a relevance
 * score, higher meaning more relevant. The normalization matters because the raw values are not comparable across
 * metrics.
 */
public enum VectorSimilarityMetric {

    COSINE(CosineSimilarity.SIMILARITY_FUNCTION) {
        @Override
        public double normalizeToRelevanceScore(float similarity) {
            // Same mapping as DenseVectorFieldMapper.VectorSimilarity#COSINE: bounded to [-1, 1], shifted to [0, 1].
            return VectorUtil.normalizeToUnitInterval(similarity);
        }
    },

    DOT_PRODUCT(DotProduct.SIMILARITY_FUNCTION) {
        @Override
        public double normalizeToRelevanceScore(float similarity) {
            // Same mapping as DenseVectorFieldMapper.VectorSimilarity#DOT_PRODUCT for float vectors.
            // Both the query and field vectors are required to be unit-length: the query vector is rejected at plan time,
            // and each field vector is rejected per-row as a warning at runtime.
            // Hence, the product only lands in [-1, 1], and so the score only lands in [0, 1].
            return VectorUtil.normalizeToUnitInterval(similarity);
        }
    },

    L2_NORM(L2Norm.SIMILARITY_FUNCTION) {
        @Override
        public double normalizeToRelevanceScore(float similarity) {
            // Same mapping as DenseVectorFieldMapper.VectorSimilarity#L2_NORM for float vectors: the raw value is
            // the Euclidean distance, and 1 / (1 + d²) makes closer vectors score higher.
            return 1.0 / (1.0 + (double) similarity * similarity);
        }
    },

    MAX_INNER_PRODUCT(DotProduct.SIMILARITY_FUNCTION) {
        @Override
        public double normalizeToRelevanceScore(float similarity) {
            // Same mapping as DenseVectorFieldMapper.VectorSimilarity#MAX_INNER_PRODUCT for float vectors. Unlike
            // DOT_PRODUCT this does not require unit-length vectors: the score grows without bound for positive
            // inner products (similarity + 1) and collapses to (0, 1) for negative ones (1 / (1 - similarity)).
            return similarity < 0 ? 1.0 / (1.0 - similarity) : (double) similarity + 1.0;
        }
    };

    private static final Map<String, VectorSimilarityMetric> BY_OPTION_VALUE = Arrays.stream(values())
        .collect(Collectors.toUnmodifiableMap(value -> value.name().toLowerCase(Locale.ROOT), Function.identity()));
    private static final List<String> OPTION_VALUES = Arrays.stream(values()).map(value -> value.name().toLowerCase(Locale.ROOT)).toList();

    private final DenseVectorFieldMapper.SimilarityFunction similarityFunction;

    VectorSimilarityMetric(DenseVectorFieldMapper.SimilarityFunction similarityFunction) {
        this.similarityFunction = similarityFunction;
    }

    /**
     * The raw, un-normalized similarity between two vectors. Whether a higher value means more or less similar
     * depends on the metric - use {@link #normalizeToRelevanceScore} to compare or rank values.
     */
    public float calculateSimilarity(float[] leftVector, float[] rightVector) {
        return similarityFunction.calculateSimilarity(leftVector, rightVector);
    }

    /**
     * Normalizes a raw similarity value into a relevance score, higher meaning more relevant, so that scores are
     * comparable across metrics and can be ranked the same way regardless of which metric produced them. Most
     * metrics produce scores in {@code [0, 1]}; {@link #MAX_INNER_PRODUCT} is unbounded above 1 for positive inner
     * products, mirroring the behavior of {@link DenseVectorFieldMapper.VectorSimilarity#MAX_INNER_PRODUCT}.
     */
    public abstract double normalizeToRelevanceScore(float similarity);

    /** Resolves an option value to its metric, ignoring case, or {@code null} when the value names no metric. */
    public static VectorSimilarityMetric fromOptionValue(String optionValue) {
        return optionValue == null ? null : BY_OPTION_VALUE.get(optionValue.toLowerCase(Locale.ROOT));
    }

    /** Every accepted option value, in declaration order, for use in error messages. */
    public static List<String> optionValues() {
        return OPTION_VALUES;
    }
}
