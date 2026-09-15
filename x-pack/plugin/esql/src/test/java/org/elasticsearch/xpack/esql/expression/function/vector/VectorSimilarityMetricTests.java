/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.vector;

import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Locale;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;

public class VectorSimilarityMetricTests extends ESTestCase {

    public void testOptionValuesMatchTheSimilarityFunctionNames() {
        assertThat(
            VectorSimilarityMetric.optionValues(),
            equalTo(List.of("cosine", "dot_product", "l2_norm", "max_inner_product"))
        );
    }

    public void testFromOptionValueIgnoresCase() {
        for (VectorSimilarityMetric metric : VectorSimilarityMetric.values()) {
            assertThat(VectorSimilarityMetric.fromOptionValue(metric.optionValue()), equalTo(metric));
            assertThat(VectorSimilarityMetric.fromOptionValue(metric.optionValue().toUpperCase(Locale.ROOT)), equalTo(metric));
            assertThat(VectorSimilarityMetric.fromOptionValue(randomCasing(metric.optionValue())), equalTo(metric));
        }
    }

    public void testFromOptionValueRejectsUnknownNames() {
        // The old v_-prefixed names are deliberately not accepted
        assertThat(VectorSimilarityMetric.fromOptionValue("v_cosine"), nullValue());
        assertThat(VectorSimilarityMetric.fromOptionValue("v_dot_product"), nullValue());
        assertThat(VectorSimilarityMetric.fromOptionValue("hamming"), nullValue());
        assertThat(VectorSimilarityMetric.fromOptionValue("l1_norm"), nullValue());
        assertThat(VectorSimilarityMetric.fromOptionValue(""), nullValue());
        assertThat(VectorSimilarityMetric.fromOptionValue(null), nullValue());
    }

    public void testCosineScore() {
        // Identical, orthogonal and opposite vectors map to the ends and the middle of the interval
        assertThat(VectorSimilarityMetric.COSINE.score(1.0f, 3), equalTo(1.0));
        assertThat(VectorSimilarityMetric.COSINE.score(0.0f, 3), equalTo(0.5));
        assertThat(VectorSimilarityMetric.COSINE.score(-1.0f, 3), equalTo(0.0));
    }

    public void testDotProductScoreOnUnitLengthVectors() {
        assertThat(VectorSimilarityMetric.DOT_PRODUCT.score(1.0f, 3), equalTo(1.0));
        assertThat(VectorSimilarityMetric.DOT_PRODUCT.score(0.0f, 3), equalTo(0.5));
        assertThat(VectorSimilarityMetric.DOT_PRODUCT.score(-1.0f, 3), equalTo(0.0));
    }

    public void testL2NormScoreDecreaseWithDistance() {
        assertThat(VectorSimilarityMetric.L2_NORM.score(0.0f, 3), equalTo(1.0));
        assertThat(VectorSimilarityMetric.L2_NORM.score(1.0f, 3), equalTo(0.5));
        assertThat(VectorSimilarityMetric.L2_NORM.score(2.0f, 3), equalTo(0.2));
    }

    public void testMaxInnerProductScore() {
        // Positive inner products: similarity + 1
        assertThat(VectorSimilarityMetric.MAX_INNER_PRODUCT.score(0.0f, 3), equalTo(1.0));
        assertThat(VectorSimilarityMetric.MAX_INNER_PRODUCT.score(1.0f, 3), equalTo(2.0));
        assertThat(VectorSimilarityMetric.MAX_INNER_PRODUCT.score(3.0f, 3), equalTo(4.0));
        // Negative inner products: 1 / (1 - similarity)
        assertThat(VectorSimilarityMetric.MAX_INNER_PRODUCT.score(-1.0f, 3), equalTo(0.5));
        assertThat(VectorSimilarityMetric.MAX_INNER_PRODUCT.score(-3.0f, 3), equalTo(0.25));
    }

    /**
     * The point of the normalization is that a higher score always means more relevant, whichever metric produced
     * it. Cosine, dot-product and max-inner-product grow as vectors get closer, while L2 grows as they get further
     * apart, so the normalization has to invert the last group.
     */
    public void testScoreOrdersByRelevanceWhicheverWayTheRawValueRuns() {
        int dimensions = randomIntBetween(1, 16);
        assertScoreIncreasesWithRawValue(VectorSimilarityMetric.COSINE, dimensions);
        assertScoreIncreasesWithRawValue(VectorSimilarityMetric.DOT_PRODUCT, dimensions);
        assertScoreIncreasesWithRawValue(VectorSimilarityMetric.MAX_INNER_PRODUCT, dimensions);
        assertScoreDecreasesWithRawValue(VectorSimilarityMetric.L2_NORM, dimensions);
    }

    private static void assertScoreIncreasesWithRawValue(VectorSimilarityMetric metric, int dimensions) {
        assertThat(metric.toString(), metric.score(0.75f, dimensions), greaterThan(metric.score(0.25f, dimensions)));
        assertThat(metric.toString(), metric.score(0.25f, dimensions), greaterThan(metric.score(-0.5f, dimensions)));
    }

    private static void assertScoreDecreasesWithRawValue(VectorSimilarityMetric metric, int dimensions) {
        assertThat(metric.toString(), metric.score(0.0f, dimensions), greaterThan(metric.score(1.0f, dimensions)));
        assertThat(metric.toString(), metric.score(1.0f, dimensions), greaterThan(metric.score(2.0f, dimensions)));
    }

    /**
     * Cosine and L2 bound their score to [0, 1] for arbitrary vectors. Dot product only does so for unit-length
     * vectors (checked separately). Max inner product is unbounded above 1 for positive inner products.
     */
    public void testCosineAndL2ScoresStayWithinTheUnitInterval() {
        int dimensions = randomIntBetween(1, 16);
        for (VectorSimilarityMetric metric : List.of(VectorSimilarityMetric.COSINE, VectorSimilarityMetric.L2_NORM)) {
            for (int i = 0; i < 100; i++) {
                double score = score(metric, randomPositiveVector(dimensions), randomPositiveVector(dimensions), dimensions);
                assertThat(metric.toString(), score, greaterThanOrEqualTo(0.0));
                assertThat(metric.toString(), score, lessThanOrEqualTo(1.0));
            }
        }
    }

    public void testDotProductScoreStaysWithinTheUnitIntervalForUnitLengthVectors() {
        int dimensions = randomIntBetween(1, 16);
        for (int i = 0; i < 100; i++) {
            float[] left = unitVector(dimensions);
            float[] right = unitVector(dimensions);
            double score = VectorSimilarityMetric.DOT_PRODUCT.score(
                VectorSimilarityMetric.DOT_PRODUCT.calculateSimilarity(left, right),
                dimensions
            );
            // A tolerance is needed because the dot product of two unit vectors only lands in [-1, 1] up to the
            // rounding of the float arithmetic that produced them
            assertThat(score, greaterThanOrEqualTo(-1e-6));
            assertThat(score, lessThanOrEqualTo(1 + 1e-6));
        }
    }

    public void testCosineScoreMatchesTheRawSimilarity() {
        float[] left = new float[] { 3.0f, 4.0f, 0.0f };
        float[] right = new float[] { 3.0f, 0.0f, 0.0f };
        // cos = 3 / 5 = 0.6, so the score is (1 + 0.6) / 2 = 0.8
        assertThat(
            VectorSimilarityMetric.COSINE.score(VectorSimilarityMetric.COSINE.calculateSimilarity(left, right), 3),
            closeTo(0.8, 1e-6)
        );
    }

    public void testMaxInnerProductScoreIsUnboundedAboveForPositiveProducts() {
        float[] a = new float[] { 10.0f, 0.0f };
        float[] b = new float[] { 10.0f, 0.0f };
        // dot product = 100, score = 101
        assertThat(
            VectorSimilarityMetric.MAX_INNER_PRODUCT.score(
                VectorSimilarityMetric.MAX_INNER_PRODUCT.calculateSimilarity(a, b),
                2
            ),
            closeTo(101.0, 1e-4)
        );
    }

    private static double score(VectorSimilarityMetric metric, float[] left, float[] right, int dimensions) {
        return metric.score(metric.calculateSimilarity(left, right), dimensions);
    }

    private static float[] randomPositiveVector(int dimensions) {
        float[] vector = new float[dimensions];
        for (int i = 0; i < dimensions; i++) {
            vector[i] = randomIntBetween(1, 100);
        }
        return vector;
    }

    private static float[] unitVector(int dimensions) {
        float[] vector = new float[dimensions];
        double squaredMagnitude = 0;
        while (squaredMagnitude == 0) {
            for (int i = 0; i < dimensions; i++) {
                vector[i] = randomFloat() - 0.5f;
                squaredMagnitude += (double) vector[i] * vector[i];
            }
        }
        float magnitude = (float) Math.sqrt(squaredMagnitude);
        for (int i = 0; i < dimensions; i++) {
            vector[i] /= magnitude;
        }
        return vector;
    }

    private static String randomCasing(String value) {
        StringBuilder builder = new StringBuilder(value.length());
        for (char c : value.toCharArray()) {
            builder.append(randomBoolean() ? Character.toUpperCase(c) : Character.toLowerCase(c));
        }
        return builder.toString();
    }
}
