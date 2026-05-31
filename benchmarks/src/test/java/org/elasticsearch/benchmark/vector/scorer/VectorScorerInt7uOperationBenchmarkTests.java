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
import org.junit.AssumptionViolatedException;

import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.supportsHeapSegments;
import static org.elasticsearch.nativeaccess.jdk.ScalarOperations.dotProduct;
import static org.elasticsearch.nativeaccess.jdk.ScalarOperations.squareDistance;

public class VectorScorerInt7uOperationBenchmarkTests extends BenchmarkTest {

    private final VectorSimilarityType function;
    private final double delta = 1e-3;
    private final int size;

    public VectorScorerInt7uOperationBenchmarkTests(VectorSimilarityType function, int size) {
        this.function = function;
        this.size = size;
    }

    public void test() {
        var bench = new VectorScorerInt7uOperationBenchmark();
        bench.function = function;
        bench.size = size;
        bench.init();
        try {
            float expected = switch (function) {
                case DOT_PRODUCT -> dotProduct(bench.byteArrayA, bench.byteArrayB);
                case EUCLIDEAN -> squareDistance(bench.byteArrayA, bench.byteArrayB);
                default -> throw new AssumptionViolatedException("Not tested");
            };
            assertEquals(expected, bench.lucene(), delta);
            assertEquals(expected, bench.nativeWithNativeSeg(), delta);
            if (supportsHeapSegments()) {
                assertEquals(expected, bench.nativeWithHeapSeg(), delta);
            }
        } finally {
            bench.teardown();
        }
    }

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() throws NoSuchFieldException {
        return generateParameters(
            VectorScorerInt7uOperationBenchmark.class.getField("function"),
            VectorScorerInt7uOperationBenchmark.class.getField("size")
        );
    }
}
