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
            equalTo(List.of("v_cosine", "v_dot_product", "v_hamming", "v_l1_norm", "v_l2_norm"))
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
        // The mapping similarity names are deliberately not accepted; the option names the ES|QL function instead
        assertThat(VectorSimilarityMetric.fromOptionValue("cosine"), nullValue());
        assertThat(VectorSimilarityMetric.fromOptionValue("l2_norm"), nullValue());
        assertThat(VectorSimilarityMetric.fromOptionValue(""), nullValue());
        assertThat(VectorSimilarityMetric.fromOptionValue(null), nullValue());
    }

    public void testCosineScore() {
        // Identical, orthogonal and opposite vectors map to the ends and the middle of the interval
        assertThat(VectorSimilarityMetric.V_COSINE.score(1.0f, 3), equalTo(1.0));
        assertThat(VectorSimilarityMetric.V_COSINE.score(0.0f, 3), equalTo(0.5));
        assertThat(VectorSimilarityMetric.V_COSINE.score(-1.0f, 3), equalTo(0.0));
    }

    public void testDotProductScoreOnUnitLengthVectors() {
        assertThat(VectorSimilarityMetric.V_DOT_PRODUCT.score(1.0f, 3), equalTo(1.0));
        assertThat(VectorSimilarityMetric.V_DOT_PRODUCT.score(0.0f, 3), equalTo(0.5));
        assertThat(VectorSimilarityMetric.V_DOT_PRODUCT.score(-1.0f, 3), equalTo(0.0));
    }

    public void testHammingScoreIsTheFractionOfMatchingBits() {
        // Every dimension holds one byte, so a 3 dimensional vector is compared over 24 bits
        assertThat(VectorSimilarityMetric.V_HAMMING.score(0.0f, 3), equalTo(1.0));
        assertThat(VectorSimilarityMetric.V_HAMMING.score(12.0f, 3), equalTo(0.5));
        assertThat(VectorSimilarityMetric.V_HAMMING.score(24.0f, 3), equalTo(0.0));
    }

    public void testDistanceMetricsScoreDecreaseWithDistance() {
        assertThat(VectorSimilarityMetric.V_L1_NORM.score(0.0f, 3), equalTo(1.0));
        assertThat(VectorSimilarityMetric.V_L1_NORM.score(1.0f, 3), equalTo(0.5));
        assertThat(VectorSimilarityMetric.V_L1_NORM.score(3.0f, 3), equalTo(0.25));

        assertThat(VectorSimilarityMetric.V_L2_NORM.score(0.0f, 3), equalTo(1.0));
        assertThat(VectorSimilarityMetric.V_L2_NORM.score(1.0f, 3), equalTo(0.5));
        assertThat(VectorSimilarityMetric.V_L2_NORM.score(2.0f, 3), equalTo(0.2));
    }

    /**
     * The point of the normalization is that a higher score always means more relevant, whichever metric produced
     * it - that is what lets the evaluator rank rows and compare against a threshold without knowing the metric.
     * Cosine and dot product grow as vectors get closer, the other three grow as they get further apart, so the
     * normalization has to run the second group the other way round.
     */
    public void testScoreOrdersByRelevanceWhicheverWayTheRawValueRuns() {
        int dimensions = randomIntBetween(1, 16);
        assertScoreIncreasesWithRawValue(VectorSimilarityMetric.V_COSINE, dimensions);
        assertScoreIncreasesWithRawValue(VectorSimilarityMetric.V_DOT_PRODUCT, dimensions);
        assertScoreDecreasesWithRawValue(VectorSimilarityMetric.V_HAMMING, dimensions);
        assertScoreDecreasesWithRawValue(VectorSimilarityMetric.V_L1_NORM, dimensions);
        assertScoreDecreasesWithRawValue(VectorSimilarityMetric.V_L2_NORM, dimensions);
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
     * Every metric but dot product bounds its score to the unit interval for arbitrary vectors. Dot product only
     * does so for unit-length vectors, which is the precondition it inherits from the dot_product mapping
     * similarity, so it is checked separately with normalized input.
     */
    public void testScoresStayWithinTheUnitInterval() {
        int dimensions = randomIntBetween(1, 16);
        for (VectorSimilarityMetric metric : VectorSimilarityMetric.values()) {
            if (metric == VectorSimilarityMetric.V_DOT_PRODUCT) {
                continue;
            }
            for (int i = 0; i < 100; i++) {
                double score = score(metric, randomByteValuedVector(dimensions), randomByteValuedVector(dimensions), dimensions);
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
            double score = VectorSimilarityMetric.V_DOT_PRODUCT.score(
                VectorSimilarityMetric.V_DOT_PRODUCT.calculateSimilarity(left, right),
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
        // cos = 3 / 5, so the score is (1 + 0.6) / 2
        assertThat(
            VectorSimilarityMetric.V_COSINE.score(VectorSimilarityMetric.V_COSINE.calculateSimilarity(left, right), 3),
            closeTo(0.8, 1e-6)
        );
    }

    private static double score(VectorSimilarityMetric metric, float[] left, float[] right, int dimensions) {
        return metric.score(metric.calculateSimilarity(left, right), dimensions);
    }

    /** Values a Hamming comparison can round-trip through a byte, so that all the metrics see the same vectors. */
    private static float[] randomByteValuedVector(int dimensions) {
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
