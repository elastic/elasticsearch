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

import org.elasticsearch.benchmark.store.DirectoryType;
import org.elasticsearch.benchmark.vector.VectorImplementation;
import org.elasticsearch.simdvec.VectorSimilarityType;

public class VectorScorerInt8BulkBenchmarkTests extends BenchmarkTest {

    private final VectorSimilarityType function;
    private final float delta = 1e-3f;
    private final int dims;

    public VectorScorerInt8BulkBenchmarkTests(VectorSimilarityType function, int dims) {
        this.function = function;
        this.dims = dims;
    }

    public void testSequential() throws Exception {
        test(this::createData, this::createBenchmark, VectorScorerBulkBenchmark.AccessMode.SEQUENTIAL, delta);
    }

    public void testRandom() throws Exception {
        test(this::createData, this::createBenchmark, VectorScorerBulkBenchmark.AccessMode.RANDOM, delta);
    }

    public void testQuerySequential() throws Exception {
        testQuery(this::createData, this::createBenchmark, VectorScorerBulkBenchmark.AccessMode.SEQUENTIAL, delta);
    }

    public void testQueryRandom() throws Exception {
        testQuery(this::createData, this::createBenchmark, VectorScorerBulkBenchmark.AccessMode.RANDOM, delta);
    }

    private VectorScorerInt8BulkBenchmark.VectorData createData(VectorScorerBulkBenchmark.AccessMode accessMode) {
        return new VectorScorerInt8BulkBenchmark.VectorData(dims, 1000, 200, random(), accessMode);
    }

    private VectorScorerInt8BulkBenchmark createBenchmark(
        VectorScorerInt8BulkBenchmark.VectorData d,
        VectorImplementation impl,
        VectorScorerBulkBenchmark.AccessMode accessMode
    ) throws java.io.IOException {
        var bench = new VectorScorerInt8BulkBenchmark();
        bench.function = function;
        bench.implementation = impl;
        bench.directoryType = DirectoryType.MMAP;
        bench.dims = dims;
        bench.numVectors = 1000;
        bench.numVectorsToScore = 200;
        bench.bulkSize = 200;
        bench.accessMode = accessMode;
        bench.setup(d);
        return bench;
    }

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() throws NoSuchFieldException {
        return generateParameters(
            VectorScorerInt8BulkBenchmark.class.getField("function"),
            VectorScorerInt8BulkBenchmark.class.getField("dims")
        );
    }
}
