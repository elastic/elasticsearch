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
import org.junit.BeforeClass;
import org.openjdk.jmh.annotations.Param;

import java.util.Arrays;

import static org.elasticsearch.benchmark.vector.scorer.ScalarOperations.dotProduct;

public class VectorScorerInt7uOperationBenchmarkTests extends ESTestCase {

    private final VectorSimilarityType function;
    private final double delta = 1e-3;
    private final int size;

    public VectorScorerInt7uOperationBenchmarkTests(VectorSimilarityType function, int size) {
        this.function = function;
        this.size = size;
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
            var bench = new VectorScorerInt7uOperationBenchmark();
            bench.function = function;
            bench.size = size;
            bench.init();
            try {
                float expected = dotProduct(bench.byteArrayA, bench.byteArrayB);
                assertEquals(expected, bench.lucene(), delta);
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
            String[] size = VectorScorerInt7uOperationBenchmark.class.getField("size").getAnnotationsByType(Param.class)[0].value();
            String[] functions = VectorScorerInt7uOperationBenchmark.class.getField("function").getAnnotationsByType(Param.class)[0]
                .value();
            return () -> Arrays.stream(size)
                .map(Integer::parseInt)
                .flatMap(i -> Arrays.stream(functions).map(VectorSimilarityType::valueOf).map(f -> new Object[] { f, i }))
                .iterator();
        } catch (NoSuchFieldException e) {
            throw new AssertionError(e);
        }
    }
}
