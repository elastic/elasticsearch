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

import org.apache.lucene.index.VectorSimilarityFunction;

public class VectorScorerBQBenchmarkTests extends BenchmarkTest {

    private static final int NUM_VECTORS = VectorScorerBQBenchmark.NUM_VECTORS;
    private static final int NUM_QUERIES = VectorScorerBQBenchmark.NUM_QUERIES;

    private final float deltaPercent = 0.05f;
    private final int dims;
    private final VectorScorerBQBenchmark.DirectoryType directoryType;
    private final VectorSimilarityFunction similarityFunction;

    public VectorScorerBQBenchmarkTests(
        int dims,
        VectorScorerBQBenchmark.DirectoryType directoryType,
        VectorSimilarityFunction similarityFunction
    ) {
        this.dims = dims;
        this.directoryType = directoryType;
        this.similarityFunction = similarityFunction;
    }

    public void testSingleScalarVsVectorized() throws Exception {
        var scalar = new VectorScorerBQBenchmark();
        var vectorized = new VectorScorerBQBenchmark();
        try {
            var data = VectorScorerBQBenchmark.generateRandomVectorData(random(), dims, NUM_VECTORS, NUM_QUERIES, similarityFunction);

            scalar.implementation = VectorScorerBQBenchmark.VectorImplementation.SCALAR;
            scalar.dims = dims;
            scalar.directoryType = directoryType;
            scalar.similarityFunction = similarityFunction;
            scalar.setup(data);

            float[] expected = scalar.scoreRandom();

            vectorized.implementation = VectorScorerBQBenchmark.VectorImplementation.VECTORIZED;
            vectorized.dims = dims;
            vectorized.directoryType = directoryType;
            vectorized.similarityFunction = similarityFunction;
            vectorized.setup(data);

            float[] result = vectorized.scoreRandom();

            assertArrayEqualsPercent("single scoring, scalar VS vectorized", expected, result, deltaPercent, DEFAULT_DELTA);
        } finally {
            scalar.teardown();
            vectorized.teardown();
        }
    }

    public void testBulkScalarVsVectorized() throws Exception {
        var scalar = new VectorScorerBQBenchmark();
        var vectorized = new VectorScorerBQBenchmark();
        try {
            var data = VectorScorerBQBenchmark.generateRandomVectorData(random(), dims, NUM_VECTORS, NUM_QUERIES, similarityFunction);

            scalar.implementation = VectorScorerBQBenchmark.VectorImplementation.SCALAR;
            scalar.dims = dims;
            scalar.directoryType = directoryType;
            scalar.similarityFunction = similarityFunction;
            scalar.setup(data);

            float[] expected = scalar.bulkScoreRandom();

            vectorized.implementation = VectorScorerBQBenchmark.VectorImplementation.VECTORIZED;
            vectorized.dims = dims;
            vectorized.directoryType = directoryType;
            vectorized.similarityFunction = similarityFunction;
            vectorized.setup(data);

            float[] result = vectorized.bulkScoreRandom();

            assertArrayEqualsPercent("bulk scoring, scalar VS vectorized", expected, result, deltaPercent, DEFAULT_DELTA);
        } finally {
            scalar.teardown();
            vectorized.teardown();
        }
    }

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() throws NoSuchFieldException {
        return generateParameters(
            VectorScorerBQBenchmark.class.getField("dims"),
            VectorScorerBQBenchmark.class.getField("directoryType"),
            VectorScorerBQBenchmark.class.getField("similarityFunction")
        );
    }
}
