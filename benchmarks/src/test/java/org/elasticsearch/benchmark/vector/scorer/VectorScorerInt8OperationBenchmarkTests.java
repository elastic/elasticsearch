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

import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.supportsHeapSegments;

public class VectorScorerInt8OperationBenchmarkTests extends ESTestCase {

    private final VectorSimilarityType function;
    private final double delta;
    private final int size;

    public VectorScorerInt8OperationBenchmarkTests(VectorSimilarityType function, int size) {
        this.function = function;
        this.size = size;
        delta = 1e-3 * size;
    }

    @BeforeClass
    public static void skipWindows() {
        assumeFalse("doesn't work on windows yet", Constants.WINDOWS);
    }

    public void test() {
        for (int i = 0; i < 100; i++) {
            var bench = new VectorScorerInt8OperationBenchmark();
            bench.function = function;
            bench.size = size;
            bench.init();
            try {
                float expected = switch (function) {
                    case COSINE -> ScalarOperations.cosine(bench.bytesA, bench.bytesB);
                    case DOT_PRODUCT -> ScalarOperations.dotProduct(bench.bytesA, bench.bytesB);
                    case EUCLIDEAN -> ScalarOperations.squareDistance(bench.bytesA, bench.bytesB);
                    default -> throw new AssumptionViolatedException("Not tested");
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
            String[] size = VectorScorerInt8OperationBenchmark.class.getField("size").getAnnotationsByType(Param.class)[0].value();
            String[] functions = VectorScorerInt8OperationBenchmark.class.getField("function").getAnnotationsByType(Param.class)[0].value();
            return () -> Arrays.stream(size)
                .map(Integer::parseInt)
                .flatMap(i -> Arrays.stream(functions).map(VectorSimilarityType::valueOf).map(f -> new Object[] { f, i }))
                .iterator();
        } catch (NoSuchFieldException e) {
            throw new AssertionError(e);
        }
    }
}
