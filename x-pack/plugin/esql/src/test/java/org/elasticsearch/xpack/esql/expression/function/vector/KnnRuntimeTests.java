/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.vector;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;

/**
 * Exercises runtime KNN filtering and scoring directly so the metric-specific behavior does not require an
 * internal cluster. The blocks contain multiple positions where useful to verify that evaluators read the values
 * belonging to the requested position rather than assuming the first value starts at offset zero.
 */
public class KnnRuntimeTests extends ESTestCase {

    private static final BlockFactory BLOCK_FACTORY = TestBlockFactory.getNonBreakingInstance();

    public void testRuntimeScoreForEveryMetric() {
        assertScore(VectorSimilarityMetric.COSINE, new float[] { 3.0f, 4.0f }, new float[] { 4.0f, 3.0f }, 2.0f, 1.96);
        assertScore(VectorSimilarityMetric.DOT_PRODUCT, new float[] { 0.6f, 0.8f }, new float[] { 0.8f, 0.6f }, 2.0f, 1.96);
        assertScore(VectorSimilarityMetric.L2_NORM, new float[] { 3.0f, 4.0f }, new float[] { 0.0f, 0.0f }, 2.0f, 2.0 / 26.0);
        assertScore(VectorSimilarityMetric.MAX_INNER_PRODUCT, new float[] { 3.0f, 4.0f }, new float[] { 2.0f, 1.0f }, 2.0f, 22.0);
    }

    public void testRuntimeScoreHandlesNullAndDimensionMismatch() {
        try (FloatBlock fieldBlock = vectorBlock((float[]) null)) {
            assertThat(
                Knn.runtimeScore(0, fieldBlock, new float[] { 1.0f, 0.0f }, VectorSimilarityMetric.COSINE, 1.0f, new float[2]),
                closeTo(0.0, 0.0)
            );
        }

        try (FloatBlock fieldBlock = vectorBlock(new float[] { 1.0f })) {
            IllegalArgumentException error = expectThrows(
                IllegalArgumentException.class,
                () -> Knn.runtimeScore(0, fieldBlock, new float[] { 1.0f, 0.0f }, VectorSimilarityMetric.COSINE, 1.0f, new float[2])
            );
            assertThat(error.getMessage(), containsString("dense_vector dimensions do not match"));
        }
    }

    public void testRuntimeFilterForEveryMetric() {
        assertFilter(VectorSimilarityMetric.COSINE, new float[] { 1.0f, 0.0f }, new float[] { 1.0f, 0.0f }, 1.0f, true);
        assertFilter(VectorSimilarityMetric.COSINE, new float[] { 0.0f, 1.0f }, new float[] { 1.0f, 0.0f }, 0.5f, false);

        assertFilter(VectorSimilarityMetric.DOT_PRODUCT, new float[] { 1.0f, 0.0f }, new float[] { 1.0f, 0.0f }, 1.0f, true);
        assertFilter(VectorSimilarityMetric.DOT_PRODUCT, new float[] { 0.0f, 1.0f }, new float[] { 1.0f, 0.0f }, 0.5f, false);

        assertFilter(VectorSimilarityMetric.L2_NORM, new float[] { 3.0f, 4.0f }, new float[] { 0.0f, 0.0f }, 5.0f, true);
        assertFilter(VectorSimilarityMetric.L2_NORM, new float[] { 6.0f, 0.0f }, new float[] { 0.0f, 0.0f }, 5.0f, false);

        assertFilter(VectorSimilarityMetric.MAX_INNER_PRODUCT, new float[] { 2.0f, 0.0f }, new float[] { 1.0f, 0.0f }, 2.0f, true);
        assertFilter(VectorSimilarityMetric.MAX_INNER_PRODUCT, new float[] { 1.0f, 0.0f }, new float[] { 1.0f, 0.0f }, 2.0f, false);
    }

    public void testRuntimeFilterHandlesNoThresholdNullAndDimensionMismatch() {
        try (FloatBlock fieldBlock = vectorBlock(new float[] { 99.0f }, new float[] { 3.0f, 4.0f }, null)) {
            assertTrue(Knn.runtimeFilter(1, fieldBlock, new float[] { 1.0f, 0.0f }, VectorSimilarityMetric.COSINE, null, null));
            assertFalse(Knn.runtimeFilter(2, fieldBlock, new float[] { 1.0f, 0.0f }, VectorSimilarityMetric.COSINE, null, null));
        }

        try (FloatBlock fieldBlock = vectorBlock(new float[] { 1.0f }, new float[] { 3.0f, 4.0f })) {
            IllegalArgumentException error = expectThrows(
                IllegalArgumentException.class,
                () -> Knn.runtimeFilter(0, fieldBlock, new float[] { 1.0f, 0.0f }, VectorSimilarityMetric.COSINE, 0.0f, new float[2])
            );
            assertThat(error.getMessage(), containsString("dense_vector dimensions do not match"));
        }
    }

    public void testRuntimeFilterUnitVector() {
        float[] queryVector = new float[] { 1.0f, 0.0f };
        try (FloatBlock fieldBlock = vectorBlock(new float[] { 99.0f }, new float[] { 0.8f, 0.6f }, new float[] { 0.0f, 1.0f })) {
            assertTrue(Knn.runtimeFilterForDotProduct(1, fieldBlock, queryVector, 0.8f, new float[2]));
            assertFalse(Knn.runtimeFilterForDotProduct(2, fieldBlock, queryVector, 0.8f, new float[2]));
            assertTrue(Knn.runtimeFilterForDotProduct(1, fieldBlock, queryVector, null, new float[2]));
        }
    }

    public void testRuntimeFilterUnitVectorRejectsNonUnitVectorWithoutThreshold() {
        try (FloatBlock fieldBlock = vectorBlock(new float[] { 2.0f, 0.0f })) {
            IllegalArgumentException error = expectThrows(
                IllegalArgumentException.class,
                () -> Knn.runtimeFilterForDotProduct(
                    0,
                    fieldBlock,
                    new float[] { 1.0f, 0.0f },
                    null,
                    new float[2]
                )
            );
            assertThat(error.getMessage(), containsString("dot_product requires unit-length vectors"));
        }
    }

    public void testRuntimeFilterUnitVectorHandlesNullAndDimensionMismatch() {
        try (FloatBlock fieldBlock = vectorBlock((float[]) null)) {
            assertFalse(
                Knn.runtimeFilterForDotProduct(
                    0,
                    fieldBlock,
                    new float[] { 1.0f, 0.0f },
                    null,
                    new float[2]
                )
            );
        }

        try (FloatBlock fieldBlock = vectorBlock(new float[] { 1.0f })) {
            IllegalArgumentException error = expectThrows(
                IllegalArgumentException.class,
                () -> Knn.runtimeFilterForDotProduct(
                    0,
                    fieldBlock,
                    new float[] { 1.0f, 0.0f },
                    null,
                    new float[2]
                )
            );
            assertThat(error.getMessage(), containsString("dense_vector dimensions do not match"));
        }
    }

    private static void assertScore(
        VectorSimilarityMetric metric,
        float[] fieldVector,
        float[] queryVector,
        float boost,
        double expectedScore
    ) {
        try (FloatBlock fieldBlock = vectorBlock(new float[] { 99.0f }, fieldVector)) {
            assertThat(
                Knn.runtimeScore(1, fieldBlock, queryVector, metric, boost, new float[queryVector.length]),
                closeTo(expectedScore, 1e-6)
            );
        }
    }

    private static void assertFilter(
        VectorSimilarityMetric metric,
        float[] fieldVector,
        float[] queryVector,
        float threshold,
        boolean expected
    ) {
        try (FloatBlock fieldBlock = vectorBlock(new float[] { 99.0f }, fieldVector)) {
            if (metric == VectorSimilarityMetric.DOT_PRODUCT) {
                assertEquals(
                    expected,
                    Knn.runtimeFilterForDotProduct(1, fieldBlock, queryVector, threshold, new float[queryVector.length])
                );
            } else {
                assertEquals(expected, Knn.runtimeFilter(1, fieldBlock, queryVector, metric, threshold, new float[queryVector.length]));
            }
        }
    }

    private static FloatBlock vectorBlock(float[]... vectors) {
        try (FloatBlock.Builder builder = BLOCK_FACTORY.newFloatBlockBuilder(vectors.length)) {
            for (float[] vector : vectors) {
                if (vector == null) {
                    builder.appendNull();
                } else if (vector.length == 1) {
                    builder.appendFloat(vector[0]);
                } else {
                    builder.beginPositionEntry();
                    for (float value : vector) {
                        builder.appendFloat(value);
                    }
                    builder.endPositionEntry();
                }
            }
            return builder.build();
        }
    }
}
