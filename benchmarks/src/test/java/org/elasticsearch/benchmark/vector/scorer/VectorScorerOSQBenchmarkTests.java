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
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.test.ESTestCase;
import org.junit.BeforeClass;
import org.openjdk.jmh.annotations.Param;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.elasticsearch.common.util.CollectionUtils.appendToCopy;

public class VectorScorerOSQBenchmarkTests extends ESTestCase {

    private final float deltaPercent = 0.1f;
    private final int dims;
    private final int bits;
    private final VectorScorerOSQBenchmark.DirectoryType directoryType;
    private final VectorSimilarityFunction similarityFunction;

    public VectorScorerOSQBenchmarkTests(
        int dims,
        int bits,
        VectorScorerOSQBenchmark.DirectoryType directoryType,
        VectorSimilarityFunction similarityFunction
    ) {
        this.dims = dims;
        this.bits = bits;
        this.directoryType = directoryType;
        this.similarityFunction = similarityFunction;
    }

    @BeforeClass
    public static void skipWindows() {
        assumeFalse("doesn't work on windows yet", Constants.WINDOWS);
    }

    public void testSingleScalarVsVectorized() throws Exception {
        for (int i = 0; i < 100; i++) {
            var seed = randomLong();

            var scalar = new VectorScorerOSQBenchmark();
            var vectorized = new VectorScorerOSQBenchmark();
            try {
                scalar.implementation = VectorScorerOSQBenchmark.VectorImplementation.SCALAR;
                scalar.dims = dims;
                scalar.bits = bits;
                scalar.directoryType = directoryType;
                scalar.similarityFunction = similarityFunction;
                scalar.setup(new Random(seed));

                float[] expected = scalar.score();

                vectorized.implementation = VectorScorerOSQBenchmark.VectorImplementation.VECTORIZED;
                vectorized.dims = dims;
                vectorized.bits = bits;
                vectorized.directoryType = directoryType;
                vectorized.similarityFunction = similarityFunction;
                vectorized.setup(new Random(seed));

                float[] result = vectorized.score();

                assertArrayEqualsPercent("single scoring, scalar VS vectorized", expected, result, deltaPercent, DEFAULT_DELTA);
            } finally {
                scalar.teardown();
                vectorized.teardown();
                IOUtils.rm(scalar.tempDir);
                IOUtils.rm(vectorized.tempDir);
            }
        }
    }

    public void testBulkScalarVsVectorized() throws Exception {
        for (int i = 0; i < 100; i++) {
            var seed = randomLong();

            var scalar = new VectorScorerOSQBenchmark();
            var vectorized = new VectorScorerOSQBenchmark();
            try {

                scalar.implementation = VectorScorerOSQBenchmark.VectorImplementation.SCALAR;
                scalar.dims = dims;
                scalar.bits = bits;
                scalar.directoryType = directoryType;
                scalar.similarityFunction = similarityFunction;
                scalar.setup(new Random(seed));

                float[] expected = scalar.bulkScore();

                vectorized.implementation = VectorScorerOSQBenchmark.VectorImplementation.VECTORIZED;
                vectorized.dims = dims;
                vectorized.bits = bits;
                vectorized.directoryType = directoryType;
                vectorized.similarityFunction = similarityFunction;
                vectorized.setup(new Random(seed));

                float[] result = vectorized.bulkScore();

                assertArrayEqualsPercent("bulk scoring, scalar VS vectorized", expected, result, deltaPercent, DEFAULT_DELTA);
            } finally {
                scalar.teardown();
                vectorized.teardown();
                IOUtils.rm(scalar.tempDir);
                IOUtils.rm(vectorized.tempDir);
            }
        }
    }

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() {
        try {
            String[] dims = VectorScorerOSQBenchmark.class.getField("dims").getAnnotationsByType(Param.class)[0].value();
            String[] bits = VectorScorerOSQBenchmark.class.getField("bits").getAnnotationsByType(Param.class)[0].value();

            return () -> Arrays.stream(dims)
                .map(Integer::parseInt)
                .flatMap(d -> Arrays.stream(bits).map(Integer::parseInt).map(b -> List.<Object>of(d, b)))
                .flatMap(params -> Arrays.stream(VectorScorerOSQBenchmark.DirectoryType.values()).map(dir -> appendToCopy(params, dir)))
                .flatMap(params -> Arrays.stream(VectorSimilarityFunction.values()).map(f -> appendToCopy(params, f).toArray()))
                .iterator();
        } catch (NoSuchFieldException e) {
            throw new AssertionError(e);
        }
    }
}
