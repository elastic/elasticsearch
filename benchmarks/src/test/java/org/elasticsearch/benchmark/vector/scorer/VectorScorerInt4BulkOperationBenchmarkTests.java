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

public class VectorScorerInt4BulkOperationBenchmarkTests extends BenchmarkTest {

    private final int dims;

    public VectorScorerInt4BulkOperationBenchmarkTests(int dims) {
        this.dims = dims;
    }

    public void testBulk() {
        for (int i = 0; i < 100; i++) {
            var vectorData = new VectorScorerInt4BulkOperationBenchmark.VectorData(dims, 1000, 200, random());
            var bench = new VectorScorerInt4BulkOperationBenchmark();
            bench.dims = dims;
            bench.numVectors = 1000;
            bench.bulkSize = 200;
            bench.setup(vectorData);
            try {
                float[] single = bench.scoreSequential();
                float[] bulk = bench.scoreBulk();
                assertArrayEquals("scoreBulk vs scoreSequential", single, bulk, 0f);
            } finally {
                bench.teardown();
            }
        }
    }

    public void testBulkOffsets() {
        for (int i = 0; i < 100; i++) {
            var vectorData = new VectorScorerInt4BulkOperationBenchmark.VectorData(dims, 1000, 200, random());
            var bench = new VectorScorerInt4BulkOperationBenchmark();
            bench.dims = dims;
            bench.numVectors = 1000;
            bench.bulkSize = 200;
            bench.setup(vectorData);
            try {
                float[] single = bench.scoreRandom();
                float[] bulk = bench.scoreBulkOffsets();
                assertArrayEquals("scoreBulkOffsets vs scoreRandom", single, bulk, 0f);
            } finally {
                bench.teardown();
            }
        }
    }

    public void testBulkSparse() {
        for (int i = 0; i < 100; i++) {
            var vectorData = new VectorScorerInt4BulkOperationBenchmark.VectorData(dims, 1000, 200, random());
            var bench = new VectorScorerInt4BulkOperationBenchmark();
            bench.dims = dims;
            bench.numVectors = 1000;
            bench.bulkSize = 200;
            bench.setup(vectorData);
            try {
                float[] single = bench.scoreRandom();
                float[] bulk = bench.scoreBulkSparse();
                assertArrayEquals("scoreBulkSparse vs scoreRandom", single, bulk, 0f);
            } finally {
                bench.teardown();
            }
        }
    }

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() throws NoSuchFieldException {
        return generateParameters(VectorScorerInt4BulkOperationBenchmark.class.getField("dims"));
    }
}
