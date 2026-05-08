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

public class VectorScorerD2Q4StripedOperationBenchmarkTests extends BenchmarkTest {

    private final int dims;

    public VectorScorerD2Q4StripedOperationBenchmarkTests(int dims) {
        assert dims % 8 == 0 : "D2Q4 striped requires dims divisible by 8: " + dims;
        this.dims = dims;
    }

    private VectorScorerD2Q4StripedOperationBenchmark newBench() {
        var vectorData = new VectorScorerD2Q4StripedOperationBenchmark.VectorData(dims, 1000, 200, random());
        var bench = new VectorScorerD2Q4StripedOperationBenchmark();
        bench.dims = dims;
        bench.numVectors = 1000;
        bench.bulkSize = 200;
        bench.setup(vectorData);
        return bench;
    }

    public void testBulk() {
        for (int i = 0; i < 100; i++) {
            var bench = newBench();
            try {
                assertArrayEquals("DOT_PRODUCT", bench.scoreSingle(), bench.scoreBulk(), 0f);
            } finally {
                bench.teardown();
            }
        }
    }

    public void testBulkOffsets() {
        for (int i = 0; i < 100; i++) {
            var bench = newBench();
            try {
                assertArrayEquals("DOT_PRODUCT", bench.scoreSingleAtOrdinals(), bench.scoreBulkOffsets(), 0f);
            } finally {
                bench.teardown();
            }
        }
    }

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() throws NoSuchFieldException {
        return generateParameters(VectorScorerD2Q4StripedOperationBenchmark.class.getField("dims"));
    }
}
