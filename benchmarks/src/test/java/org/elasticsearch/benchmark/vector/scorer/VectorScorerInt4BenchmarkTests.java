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

public class VectorScorerInt4BenchmarkTests extends ESTestCase {

    private final double delta = 1e-3;
    private final VectorSimilarityType function;
    private final int dims;

    public VectorScorerInt4BenchmarkTests(VectorSimilarityType function, int dims) {
        this.function = function;
        this.dims = dims;
    }

    private VectorScorerInt4Benchmark createBench(VectorImplementation impl, VectorScorerInt4Benchmark.VectorData data) throws Exception {
        var bench = new VectorScorerInt4Benchmark();
        bench.function = function;
        bench.implementation = impl;
        bench.dims = dims;
        bench.setup(data);
        return bench;
    }

    public void testScores() throws Exception {
        for (int i = 0; i < 100; i++) {
            var data = new VectorScorerInt4Benchmark.VectorData(dims);
            var scalar = createBench(VectorImplementation.SCALAR, data);
            var lucene = createBench(VectorImplementation.LUCENE, data);
            var nativeBench = createBench(VectorImplementation.NATIVE, data);

            try {
                float expected = scalar.score();
                assertEquals("LUCENE score", expected, lucene.score(), delta);
                assertEquals("NATIVE score", expected, nativeBench.score(), delta);
            } finally {
                scalar.teardown();
                lucene.teardown();
                nativeBench.teardown();
            }
        }
    }

    public void testQueryScores() throws Exception {
        for (int i = 0; i < 100; i++) {
            var data = new VectorScorerInt4Benchmark.VectorData(dims);
            var scalar = createBench(VectorImplementation.SCALAR, data);
            var lucene = createBench(VectorImplementation.LUCENE, data);
            var nativeBench = createBench(VectorImplementation.NATIVE, data);

            try {
                float expected = scalar.scoreQuery();
                assertEquals("LUCENE scoreQuery", expected, lucene.scoreQuery(), delta);
                assertEquals("NATIVE scoreQuery", expected, nativeBench.scoreQuery(), delta);
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
            String[] dims = VectorScorerInt4Benchmark.class.getField("dims").getAnnotationsByType(Param.class)[0].value();
            String[] functions = VectorScorerInt4Benchmark.class.getField("function").getAnnotationsByType(Param.class)[0].value();
            return () -> Arrays.stream(dims)
                .map(Integer::parseInt)
                .flatMap(d -> Arrays.stream(functions).map(f -> new Object[] { VectorSimilarityType.valueOf(f), d }))
                .iterator();
        } catch (NoSuchFieldException e) {
            throw new AssertionError(e);
        }
    }
}
