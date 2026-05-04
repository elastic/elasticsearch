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

import org.elasticsearch.nativeaccess.jdk.ScalarOperations;
import org.elasticsearch.simdvec.VectorSimilarityType;
import org.junit.AssumptionViolatedException;

import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.supportsHeapSegments;

public class VectorScorerFloat32OperationBenchmarkTests extends BenchmarkTest {

    private final VectorSimilarityType function;
    private final double delta;
    private final int size;

    public VectorScorerFloat32OperationBenchmarkTests(VectorSimilarityType function, int size) {
        this.function = function;
        this.size = size;
        delta = 1e-3 * size;
    }

    public void test() {
        for (int i = 0; i < 100; i++) {
            var bench = new VectorScorerFloat32OperationBenchmark();
            bench.function = function;
            bench.size = size;
            bench.init();
            try {
                float expected = switch (function) {
                    case DOT_PRODUCT -> ScalarOperations.dotProduct(bench.floatsA, bench.floatsB);
                    case EUCLIDEAN -> ScalarOperations.squareDistance(bench.floatsA, bench.floatsB);
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
    public static Iterable<Object[]> parametersFactory() throws NoSuchFieldException {
        return generateParameters(
            VectorScorerFloat32OperationBenchmark.class.getField("function"),
            VectorScorerFloat32OperationBenchmark.class.getField("size")
        );
    }
}
