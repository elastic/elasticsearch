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
import java.util.List;
import java.util.stream.Stream;

import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.supportsHeapSegments;

public class VectorScorerInt7uBenchmarkTests extends ESTestCase {

    private final double delta = 1e-3;
    private final VectorScorerInt7uBenchmark.Function function;
    private final int dims;

    public VectorScorerInt7uBenchmarkTests(VectorScorerInt7uBenchmark.Function function, int dims) {
        this.function = function;
        this.dims = dims;
    }

    @BeforeClass
    public static void skipWindows() {
        assumeFalse("doesn't work on windows yet", Constants.WINDOWS);
    }

    public void testScores() throws Exception {
        for (int i = 0; i < 100; i++) {
            VectorScorerInt7uBenchmark.VectorData data = new VectorScorerInt7uBenchmark.VectorData(dims);
            Float expected = null;
            for (var impl : VectorScorerInt7uBenchmark.Implementation.values()) {
                var bench = new VectorScorerInt7uBenchmark();
                bench.function = function;
                bench.implementation = impl;
                bench.dims = dims;
                bench.setup(data);

                try {
                    float result = bench.score();
                    if (expected == null) {
                        assert impl == VectorScorerInt7uBenchmark.Implementation.SCALAR;
                        expected = result;
                        continue;
                    }

                    assertEquals(impl.toString(), expected, result, delta);
                } finally {
                    bench.teardown();
                }
            }
        }
    }

    public void testQueryScores() throws Exception {
        assumeTrue("Only test with heap segments", supportsHeapSegments());
        for (int i = 0; i < 100; i++) {
            VectorScorerInt7uBenchmark.VectorData data = new VectorScorerInt7uBenchmark.VectorData(dims);
            Float expected = null;
            for (var impl : List.of(VectorScorerInt7uBenchmark.Implementation.LUCENE, VectorScorerInt7uBenchmark.Implementation.NATIVE)) {
                var bench = new VectorScorerInt7uBenchmark();
                bench.function = function;
                bench.implementation = impl;
                bench.dims = dims;
                bench.setup(data);

                try {
                    float result = bench.scoreQuery();
                    if (expected == null) {
                        assert impl == VectorScorerInt7uBenchmark.Implementation.LUCENE;
                        expected = result;
                        continue;
                    }

                    assertEquals(impl.toString(), expected, bench.scoreQuery(), delta);
                } finally {
                    bench.teardown();
                }
            }
        }
    }

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() {
        try {
            var params = VectorScorerInt7uBenchmark.class.getField("dims").getAnnotationsByType(Param.class)[0].value();
            return () -> Arrays.stream(params)
                .map(Integer::parseInt)
                .flatMap(
                    i -> Stream.of(
                        new Object[] { VectorScorerInt7uBenchmark.Function.DOT_PRODUCT, i },
                        new Object[] { VectorScorerInt7uBenchmark.Function.SQUARE_DISTANCE, i }
                    )
                )
                .iterator();
        } catch (NoSuchFieldException e) {
            throw new AssertionError(e);
        }
    }
}
