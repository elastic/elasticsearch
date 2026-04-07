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
import org.apache.lucene.util.Constants;
import org.elasticsearch.test.ESTestCase;
import org.junit.BeforeClass;
import org.openjdk.jmh.annotations.Param;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.elasticsearch.common.util.CollectionUtils.appendToCopy;

public class VectorScorerBQBenchmarkTests extends ESTestCase {

    private static final int REPETITIONS = 10;
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

    @BeforeClass
    public static void skipWindows() {
        assumeFalse("doesn't work on windows yet", Constants.WINDOWS);
    }

    public void testSingleScalarVsVectorized() throws Exception {
        for (int i = 0; i < REPETITIONS; i++) {
            var seed = randomLong();

            var scalar = new VectorScorerBQBenchmark();
            var vectorized = new VectorScorerBQBenchmark();
            try {
                var data = VectorScorerBQBenchmark.generateRandomVectorData(
                    new Random(seed),
                    dims,
                    NUM_VECTORS,
                    NUM_QUERIES,
                    similarityFunction
                );

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
    }

    public void testBulkScalarVsVectorized() throws Exception {
        for (int i = 0; i < REPETITIONS; i++) {
            var seed = randomLong();

            var scalar = new VectorScorerBQBenchmark();
            var vectorized = new VectorScorerBQBenchmark();
            try {
                var data = VectorScorerBQBenchmark.generateRandomVectorData(
                    new Random(seed),
                    dims,
                    NUM_VECTORS,
                    NUM_QUERIES,
                    similarityFunction
                );

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
    }

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() {
        try {
            String[] dims = VectorScorerOSQBenchmark.class.getField("dims").getAnnotationsByType(Param.class)[0].value();
            return () -> Arrays.stream(dims)
                .map(Integer::parseInt)
                .flatMap(d -> Arrays.stream(VectorScorerBQBenchmark.DirectoryType.values()).map(dir -> List.<Object>of(d, dir)))
                .flatMap(params -> Arrays.stream(VectorSimilarityFunction.values()).map(f -> appendToCopy(params, f).toArray()))
                .iterator();
        } catch (NoSuchFieldException e) {
            throw new AssertionError(e);
        }
    }
}
