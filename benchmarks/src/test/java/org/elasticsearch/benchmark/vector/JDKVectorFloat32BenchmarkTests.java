/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.vector;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.Constants;
import org.elasticsearch.test.ESTestCase;
import org.junit.BeforeClass;
import org.openjdk.jmh.annotations.Param;

import java.util.Arrays;

public class JDKVectorFloat32BenchmarkTests extends ESTestCase {

    final double delta;
    final int size;

    public JDKVectorFloat32BenchmarkTests(int size) {
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

    public void testCosine() {
        for (int i = 0; i < 100; i++) {
            var bench = new JDKVectorFloat32Benchmark();
            bench.size = size;
            bench.init();
            try {
                float expected = cosineFloat32Scalar(bench.floatsA, bench.floatsB);
                assertEquals(expected, bench.cosineLucene(), delta);
                assertEquals(expected, bench.cosineLuceneWithCopy(), delta);
                assertEquals(expected, bench.cosineNativeWithNativeSeg(), delta);
                if (supportsHeapSegments()) {
                    assertEquals(expected, bench.cosineNativeWithHeapSeg(), delta);
                }
            } finally {
                bench.teardown();
            }
        }
    }

    public void testDotProduct() {
        for (int i = 0; i < 100; i++) {
            var bench = new JDKVectorFloat32Benchmark();
            bench.size = size;
            bench.init();
            try {
                float expected = dotProductFloat32Scalar(bench.floatsA, bench.floatsB);
                assertEquals(expected, bench.dotProductLucene(), delta);
                assertEquals(expected, bench.dotProductLuceneWithCopy(), delta);
                assertEquals(expected, bench.dotProductNativeWithNativeSeg(), delta);
                if (supportsHeapSegments()) {
                    assertEquals(expected, bench.dotProductNativeWithHeapSeg(), delta);
                }
            } finally {
                bench.teardown();
            }
        }
    }

    public void testSquareDistance() {
        for (int i = 0; i < 100; i++) {
            var bench = new JDKVectorFloat32Benchmark();
            bench.size = size;
            bench.init();
            try {
                float expected = squareDistanceFloat32Scalar(bench.floatsA, bench.floatsB);
                assertEquals(expected, bench.squareDistanceLucene(), delta);
                assertEquals(expected, bench.squareDistanceLuceneWithCopy(), delta);
                assertEquals(expected, bench.squareDistanceNativeWithNativeSeg(), delta);
                if (supportsHeapSegments()) {
                    assertEquals(expected, bench.squareDistanceNativeWithHeapSeg(), delta);
                }
            } finally {
                bench.teardown();
            }
        }
    }

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() {
        try {
            var params = JDKVectorFloat32Benchmark.class.getField("size").getAnnotationsByType(Param.class)[0].value();
            return () -> Arrays.stream(params).map(Integer::parseInt).map(i -> new Object[] { i }).iterator();
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
