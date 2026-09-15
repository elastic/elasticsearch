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
 * Each constant pairs the raw similarity computation - the same one the equally named ES|QL similarity function
 * exposes, so {@code v_cosine} here computes what {@link CosineSimilarity} computes - with the normalization that
 * turns that raw value into a relevance score in the {@code [0, 1]} interval, higher meaning more relevant. The
 * normalization matters because the raw values are not comparable across metrics, and half of them run the wrong
 * way: cosine and dot product grow as vectors get closer, while the L1/L2 distances and the Hamming bit count grow
 * as they get further apart.
 */
public enum VectorSimilarityMetric {

    V_COSINE(CosineSimilarity.SIMILARITY_FUNCTION) {
        @Override
        public double score(float similarity, int dimensions) {
            // Cosine is bounded to [-1, 1], so shifting it into [0, 1] is all that is needed. Same mapping as
            // DenseVectorFieldMapper.VectorSimilarity#COSINE.
            return VectorUtil.normalizeToUnitInterval(similarity);
        }
    },

    V_DOT_PRODUCT(DotProduct.SIMILARITY_FUNCTION) {
        @Override
        public double score(float similarity, int dimensions) {
            // Same mapping as DenseVectorFieldMapper.VectorSimilarity#DOT_PRODUCT for float vectors. The raw dot
            // product only lands in [-1, 1], and so the score only lands in [0, 1], for unit-length vectors.
            // Both the query and field vectors are required to be unit-length: the query vector is rejected at
            // plan time, and each field vector is rejected per-row as a warning at runtime.
            return VectorUtil.normalizeToUnitInterval(similarity);
        }
    },

    V_HAMMING(Hamming.EVALUATOR_SIMILARITY_FUNCTION) {
        @Override
        public double score(float similarity, int dimensions) {
            // The raw value is the number of differing bits. ESVectorUtil#hammingScore turns that into the fraction
            // of bits that match, which is what we replicate here: every dimension of a Hamming vector holds a byte.
            int bits = dimensions * Byte.SIZE;
            return (bits - (double) similarity) / bits;
        }
    },

    V_L1_NORM(L1Norm.SIMILARITY_FUNCTION) {
        @Override
        public double score(float similarity, int dimensions) {
            // The raw value is a distance in [0, inf), so it is inverted to make closer vectors score higher. There
            // is no mapping for L1 in DenseVectorFieldMapper.VectorSimilarity; this mirrors the 1 / (1 + d^2) that
            // L2_NORM uses there, with the squaring dropped as L1 is already a sum of absolute differences.
            return 1.0 / (1.0 + similarity);
        }
    },

    V_L2_NORM(L2Norm.SIMILARITY_FUNCTION) {
        @Override
        public double score(float similarity, int dimensions) {
            // Same mapping as DenseVectorFieldMapper.VectorSimilarity#L2_NORM: the raw value is the euclidean
            // distance, whose square is what that mapping is defined over.
            return 1.0 / (1.0 + (double) similarity * similarity);
        }
    };

    private static final Map<String, VectorSimilarityMetric> BY_OPTION_VALUE = Arrays.stream(values())
        .collect(Collectors.toUnmodifiableMap(VectorSimilarityMetric::optionValue, Function.identity()));

    private final DenseVectorFieldMapper.SimilarityFunction similarityFunction;

    VectorSimilarityMetric(DenseVectorFieldMapper.SimilarityFunction similarityFunction) {
        this.similarityFunction = similarityFunction;
    }

    /**
     * The raw, un-normalized similarity between two vectors. Whether a higher value means more or less similar
     * depends on the metric - use {@link #score} to compare or rank values.
     */
    public float calculateSimilarity(float[] leftVector, float[] rightVector) {
        return similarityFunction.calculateSimilarity(leftVector, rightVector);
    }

    /**
     * Normalizes a raw similarity value into a relevance score in the {@code [0, 1]} interval, higher meaning more
     * relevant, so that scores are comparable across metrics and can be ranked the same way regardless of which
     * metric produced them. {@link #V_DOT_PRODUCT} only stays inside the interval for unit-length vectors - see
     * its own note.
     *
     * @param dimensions the number of dimensions of the vectors the similarity was computed over; only metrics whose
     *                   range depends on the vector length (Hamming) use it
     */
    public abstract double score(float similarity, int dimensions);

    /** The name this metric is referred to by in the {@code vector_similarity} option, e.g. {@code v_cosine}. */
    public String optionValue() {
        return name().toLowerCase(Locale.ROOT);
    }

    /** Resolves an option value to its metric, ignoring case, or {@code null} when the value names no metric. */
    public static VectorSimilarityMetric fromOptionValue(String optionValue) {
        return optionValue == null ? null : BY_OPTION_VALUE.get(optionValue.toLowerCase(Locale.ROOT));
    }

    /** Every accepted option value, in declaration order, for use in error messages. */
    public static List<String> optionValues() {
        return Arrays.stream(values()).map(VectorSimilarityMetric::optionValue).toList();
    }
}
