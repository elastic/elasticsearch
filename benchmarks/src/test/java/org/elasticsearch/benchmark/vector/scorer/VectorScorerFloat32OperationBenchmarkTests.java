/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.vector.scorer;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.Constants;
import org.elasticsearch.simdvec.VectorSimilarityType;
import org.elasticsearch.test.ESTestCase;
import org.junit.AssumptionViolatedException;
import org.junit.BeforeClass;
import org.openjdk.jmh.annotations.Param;

import java.util.Arrays;

public class VectorScorerFloat32OperationBenchmarkTests extends ESTestCase {

    private VectorSimilarityType function;
    private final double delta;
    private final int size;

    public VectorScorerFloat32OperationBenchmarkTests(VectorSimilarityType function, int size) {
        this.function = function;
        this.size = size;
        delta = 1e-3 * size;
    }

    @BeforeClass
    public static void skipWindows() {
        assumeFalse("doesn't work on windows yet", Constants.WINDOWS);
    }

    static boolean supportsHeapSegments() {
        return Runtime.version().feature() >= 22;
    }

    public void test() {
        for (int i = 0; i < 100; i++) {
            var bench = new VectorScorerFloat32OperationBenchmark();
            bench.function = function;
            bench.size = size;
            bench.init();
            try {
                float expected = switch (function) {
                    case COSINE -> cosineFloat32Scalar(bench.floatsA, bench.floatsB);
                    case DOT_PRODUCT -> dotProductFloat32Scalar(bench.floatsA, bench.floatsB);
                    case EUCLIDEAN -> squareDistanceFloat32Scalar(bench.floatsA, bench.floatsB);
                    case MAXIMUM_INNER_PRODUCT -> throw new AssumptionViolatedException("Not tested");
                };
                assertEquals(expected, bench.lucene(), delta);
                assertEquals(expected, bench.luceneWithCopy(), delta);
                assertEquals(expected, bench.nativeWithNativeSeg(), delta);
                if (supportsHeapSegments()) {
                    assertEquals(expected, bench.nativeWithHeapSeg(), delta);
                }
            } finally {
                bench.teardown();
            }
        }
    }

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() {
        try {
            String[] size = VectorScorerFloat32OperationBenchmark.class.getField("size").getAnnotationsByType(Param.class)[0].value();
            String[] functions = VectorScorerFloat32OperationBenchmark.class.getField("function").getAnnotationsByType(Param.class)[0]
                .value();
            return () -> Arrays.stream(size)
                .map(Integer::parseInt)
                .flatMap(i -> Arrays.stream(functions).map(VectorSimilarityType::valueOf).map(f -> new Object[] { f, i }))
                .iterator();
        } catch (NoSuchFieldException e) {
            throw new AssertionError(e);
        }
    }

    /** Computes the cosine of the given vectors a and b. */
    static float cosineFloat32Scalar(float[] a, float[] b) {
        float dot = 0, normA = 0, normB = 0;
        for (int i = 0; i < a.length; i++) {
            dot += a[i] * b[i];
            normA += a[i] * a[i];
            normB += b[i] * b[i];
        }
        double normAA = Math.sqrt(normA);
        double normBB = Math.sqrt(normB);
        if (normAA == 0.0f || normBB == 0.0f) return 0.0f;
        return (float) (dot / (normAA * normBB));
    }

    /** Computes the dot product of the given vectors a and b. */
    static float dotProductFloat32Scalar(float[] a, float[] b) {
        float res = 0;
        for (int i = 0; i < a.length; i++) {
            res += a[i] * b[i];
        }
        return res;
    }

    /** Computes the dot product of the given vectors a and b. */
    static float squareDistanceFloat32Scalar(float[] a, float[] b) {
        float squareSum = 0;
        for (int i = 0; i < a.length; i++) {
            float diff = a[i] - b[i];
            squareSum += diff * diff;
        }
        return squareSum;
    }
}
