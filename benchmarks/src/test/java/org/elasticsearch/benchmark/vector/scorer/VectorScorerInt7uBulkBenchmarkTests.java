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
import org.elasticsearch.test.ESTestCase;
import org.junit.BeforeClass;
import org.openjdk.jmh.annotations.Param;

import java.util.Arrays;

public class VectorScorerInt7uBulkBenchmarkTests extends ESTestCase {

    final float delta = 1e-3f;
    final int dims;

    public VectorScorerInt7uBulkBenchmarkTests(int dims) {
        this.dims = dims;
    }

    @BeforeClass
    public static void skipWindows() {
        assumeFalse("doesn't work on windows yet", Constants.WINDOWS);
    }

    public void testDotProductSequential() throws Exception {
        for (int i = 0; i < 100; i++) {
            var bench = new VectorScorerInt7uBulkBenchmark();
            bench.dims = dims;
            bench.numVectors = 1000;
            bench.numVectorsToScore = 200;
            bench.setup();
            try {
                float[] expected = bench.dotProductScalarMultipleSequential();
                assertArrayEquals(expected, bench.dotProductLuceneMultipleSequential(), delta);
                assertArrayEquals(expected, bench.dotProductNativeMultipleSequential(), delta);
            } finally {
                bench.teardown();
            }
        }
    }

    public void testDotProductRandom() throws Exception {
        for (int i = 0; i < 100; i++) {
            var bench = new VectorScorerInt7uBulkBenchmark();
            bench.dims = dims;
            bench.numVectors = 1000;
            bench.numVectorsToScore = 200;
            bench.setup();
            try {
                float[] expected = bench.dotProductScalarMultipleRandom();
                assertArrayEquals(expected, bench.dotProductLuceneMultipleRandom(), delta);
                assertArrayEquals(expected, bench.dotProductNativeMultipleRandom(), delta);
                assertArrayEquals(expected, bench.dotProductNativeMultipleRandomBulk(), delta);
            } finally {
                bench.teardown();
            }
        }
    }

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() {
        try {
            var params = VectorScorerInt7uBulkBenchmark.class.getField("dims").getAnnotationsByType(Param.class)[0].value();
            return () -> Arrays.stream(params).map(Integer::parseInt).map(i -> new Object[] { i }).iterator();
        } catch (NoSuchFieldException e) {
            throw new AssertionError(e);
        }
    }
}
