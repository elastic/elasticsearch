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

import org.junit.BeforeClass;

import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.supportsHeapSegments;

public class VectorScorerInt2OperationBenchmarkTests extends BenchmarkTest {

    private final int size;

    public VectorScorerInt2OperationBenchmarkTests(int size) {
        this.size = size;
    }

    @BeforeClass
    public static void skipUnsupported() {
        assumeTrue("native requires JDK22+", supportsHeapSegments());
    }

    public void test() {
        for (int i = 0; i < 100; i++) {
            var bench = new VectorScorerInt2OperationBenchmark();
            bench.size = size;
            bench.init();

            int expected = bench.scalar();
            assertEquals(expected, bench.nativeWithNativeSeg());
            assertEquals(expected, bench.nativeWithHeapSeg());
        }
    }

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() throws NoSuchFieldException {
        return generateParameters(VectorScorerInt2OperationBenchmark.class.getField("size"));
    }
}
