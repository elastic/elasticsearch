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

import org.elasticsearch.simdvec.VectorSimilarityType;
import org.elasticsearch.test.ESTestCase;
import org.openjdk.jmh.annotations.Param;

import java.util.Arrays;

public class VectorScorerInt4BulkBenchmarkTests extends ESTestCase {

    private final VectorSimilarityType function;
    private final float delta = 1e-3f;
    private final int dims;

    public VectorScorerInt4BulkBenchmarkTests(VectorSimilarityType function, int dims) {
        this.function = function;
        this.dims = dims;
    }

    private VectorScorerInt4BulkBenchmark createBench(VectorImplementation impl, VectorScorerInt4BulkBenchmark.VectorData vectorData)
        throws Exception {
        var bench = new VectorScorerInt4BulkBenchmark();
        bench.function = function;
        bench.implementation = impl;
        bench.dims = dims;
        bench.numVectors = 1000;
        bench.numVectorsToScore = 200;
        bench.bulkSize = 200;
        bench.setup(vectorData);
        return bench;
    }

    public void testSequential() throws Exception {
        for (int i = 0; i < 100; i++) {
            var vectorData = new VectorScorerInt4BulkBenchmark.VectorData(dims, 1000, 200);
            var scalar = createBench(VectorImplementation.SCALAR, vectorData);
            var lucene = createBench(VectorImplementation.LUCENE, vectorData);
            var nativeBench = createBench(VectorImplementation.NATIVE, vectorData);

            try {
                float[] expected = scalar.scoreMultipleSequential();
                assertArrayEquals("LUCENE sequential", expected, lucene.scoreMultipleSequential(), delta);
                assertArrayEquals("NATIVE sequential", expected, nativeBench.scoreMultipleSequential(), delta);
            } finally {
                scalar.teardown();
                lucene.teardown();
                nativeBench.teardown();
            }
        }
    }

    public void testRandom() throws Exception {
        for (int i = 0; i < 100; i++) {
            var vectorData = new VectorScorerInt4BulkBenchmark.VectorData(dims, 1000, 200);
            var scalar = createBench(VectorImplementation.SCALAR, vectorData);
            var lucene = createBench(VectorImplementation.LUCENE, vectorData);
            var nativeBench = createBench(VectorImplementation.NATIVE, vectorData);

            try {
                float[] expected = scalar.scoreMultipleRandom();
                assertArrayEquals("LUCENE random", expected, lucene.scoreMultipleRandom(), delta);
                assertArrayEquals("NATIVE random", expected, nativeBench.scoreMultipleRandom(), delta);
            } finally {
                scalar.teardown();
                lucene.teardown();
                nativeBench.teardown();
            }
        }
    }

    public void testQueryRandom() throws Exception {
        for (int i = 0; i < 100; i++) {
            var vectorData = new VectorScorerInt4BulkBenchmark.VectorData(dims, 1000, 200);
            var scalar = createBench(VectorImplementation.SCALAR, vectorData);
            var lucene = createBench(VectorImplementation.LUCENE, vectorData);
            var nativeBench = createBench(VectorImplementation.NATIVE, vectorData);

            try {
                float[] expected = scalar.scoreQueryMultipleRandom();
                assertArrayEquals("LUCENE queryRandom", expected, lucene.scoreQueryMultipleRandom(), delta);
                assertArrayEquals("NATIVE queryRandom", expected, nativeBench.scoreQueryMultipleRandom(), delta);
            } finally {
                scalar.teardown();
                lucene.teardown();
                nativeBench.teardown();
            }
        }
    }

    public void testSequentialBulk() throws Exception {
        for (int i = 0; i < 100; i++) {
            var vectorData = new VectorScorerInt4BulkBenchmark.VectorData(dims, 1000, 200);
            var scalar = createBench(VectorImplementation.SCALAR, vectorData);
            var lucene = createBench(VectorImplementation.LUCENE, vectorData);
            var nativeBench = createBench(VectorImplementation.NATIVE, vectorData);

            try {
                float[] expected = scalar.scoreMultipleSequentialBulk();
                assertArrayEquals("LUCENE sequentialBulk", expected, lucene.scoreMultipleSequentialBulk(), delta);
                assertArrayEquals("NATIVE sequentialBulk", expected, nativeBench.scoreMultipleSequentialBulk(), delta);
            } finally {
                scalar.teardown();
                lucene.teardown();
                nativeBench.teardown();
            }
        }
    }

    public void testRandomBulk() throws Exception {
        for (int i = 0; i < 100; i++) {
            var vectorData = new VectorScorerInt4BulkBenchmark.VectorData(dims, 1000, 200);
            var scalar = createBench(VectorImplementation.SCALAR, vectorData);
            var lucene = createBench(VectorImplementation.LUCENE, vectorData);
            var nativeBench = createBench(VectorImplementation.NATIVE, vectorData);

            try {
                float[] expected = scalar.scoreMultipleRandomBulk();
                assertArrayEquals("LUCENE randomBulk", expected, lucene.scoreMultipleRandomBulk(), delta);
                assertArrayEquals("NATIVE randomBulk", expected, nativeBench.scoreMultipleRandomBulk(), delta);
            } finally {
                scalar.teardown();
                lucene.teardown();
                nativeBench.teardown();
            }
        }
    }

    public void testQueryRandomBulk() throws Exception {
        for (int i = 0; i < 100; i++) {
            var vectorData = new VectorScorerInt4BulkBenchmark.VectorData(dims, 1000, 200);
            var scalar = createBench(VectorImplementation.SCALAR, vectorData);
            var lucene = createBench(VectorImplementation.LUCENE, vectorData);
            var nativeBench = createBench(VectorImplementation.NATIVE, vectorData);

            try {
                float[] expected = scalar.scoreQueryMultipleRandomBulk();
                assertArrayEquals("LUCENE queryRandomBulk", expected, lucene.scoreQueryMultipleRandomBulk(), delta);
                assertArrayEquals("NATIVE queryRandomBulk", expected, nativeBench.scoreQueryMultipleRandomBulk(), delta);
            } finally {
                scalar.teardown();
                lucene.teardown();
                nativeBench.teardown();
            }
        }
    }

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() {
        try {
            String[] dims = VectorScorerInt4BulkBenchmark.class.getField("dims").getAnnotationsByType(Param.class)[0].value();
            String[] functions = VectorScorerInt4BulkBenchmark.class.getField("function").getAnnotationsByType(Param.class)[0].value();
            return () -> Arrays.stream(dims)
                .map(Integer::parseInt)
                .flatMap(d -> Arrays.stream(functions).map(f -> new Object[] { VectorSimilarityType.valueOf(f), d }))
                .iterator();
        } catch (NoSuchFieldException e) {
            throw new AssertionError(e);
        }
    }
}
