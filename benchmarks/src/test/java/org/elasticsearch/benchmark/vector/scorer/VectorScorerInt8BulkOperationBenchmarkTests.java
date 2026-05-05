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

public class VectorScorerInt8BulkOperationBenchmarkTests extends BenchmarkTest {

    private final VectorSimilarityType function;
    private final int dims;

    public VectorScorerInt8BulkOperationBenchmarkTests(VectorSimilarityType function, int dims) {
        this.function = function;
        this.dims = dims;
    }

    public void testSequential() {
        for (int i = 0; i < 100; i++) {
            var vectorData = new VectorScorerInt8BulkOperationBenchmark.VectorData(dims, 1000, 200, random());
            var bench = new VectorScorerInt8BulkOperationBenchmark();
            bench.function = function;
            bench.dims = dims;
            bench.numVectors = 1000;
            bench.bulkSize = 200;
            bench.setup(vectorData);
            try {
                float[] single = bench.scoreMultipleSequential();
                float[] bulk = bench.scoreMultipleSequentialBulk();
                assertArrayEquals(function.toString(), single, bulk, 1e-5f);
            } finally {
                bench.teardown();
            }
        }
    }

    public void testRandom() {
        for (int i = 0; i < 100; i++) {
            var vectorData = new VectorScorerInt8BulkOperationBenchmark.VectorData(dims, 1000, 200, random());
            var bench = new VectorScorerInt8BulkOperationBenchmark();
            bench.function = function;
            bench.dims = dims;
            bench.numVectors = 1000;
            bench.bulkSize = 200;
            bench.setup(vectorData);
            try {
                float[] single = bench.scoreMultipleRandom();
                float[] bulk = bench.scoreMultipleRandomBulk();
                assertArrayEquals(function.toString(), single, bulk, 1e-5f);
            } finally {
                bench.teardown();
            }
        }
    }

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() throws NoSuchFieldException {
        return generateParameters(
            VectorScorerInt8BulkOperationBenchmark.class.getField("function"),
            VectorScorerInt8BulkOperationBenchmark.class.getField("dims")
        );
    }
}
