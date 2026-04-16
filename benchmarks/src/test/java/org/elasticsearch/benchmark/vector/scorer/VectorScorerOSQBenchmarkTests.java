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
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.simdvec.ES940OSQVectorsScorer;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.util.Random;

@TestLogging(
    reason = "Noisy logging",
    value = "org.elasticsearch.env.NodeEnvironment:WARN,org.elasticsearch.xpack.searchablesnapshots.cache.full.PersistentCache:WARN"
)
public class VectorScorerOSQBenchmarkTests extends BenchmarkTest {

    private static final int REPETITIONS = 10;
    private final float deltaPercent = 0.1f;
    private final int dims;
    private final byte bits;
    private final VectorScorerOSQBenchmark.DirectoryType directoryType;
    private final ES940OSQVectorsScorer.SymmetricInt4Encoding int4Encoding;
    private final VectorSimilarityFunction similarityFunction;

    public VectorScorerOSQBenchmarkTests(
        int dims,
        byte bits,
        VectorScorerOSQBenchmark.DirectoryType directoryType,
        ES940OSQVectorsScorer.SymmetricInt4Encoding int4Encoding,
        VectorSimilarityFunction similarityFunction
    ) {
        this.dims = dims;
        this.bits = bits;
        this.directoryType = directoryType;
        this.int4Encoding = int4Encoding;
        this.similarityFunction = similarityFunction;
    }

    public void testSingleScalarVsVectorized() throws Exception {
        for (int i = 0; i < REPETITIONS; i++) {
            var seed = randomLong();

            var scalar = new VectorScorerOSQBenchmark();
            var vectorized = new VectorScorerOSQBenchmark();
            try {
                var data = VectorScorerOSQBenchmark.generateRandomVectorData(
                    new Random(seed),
                    dims,
                    bits,
                    int4Encoding,
                    similarityFunction
                );

                scalar.implementation = VectorScorerOSQBenchmark.VectorImplementation.SCALAR;
                scalar.dims = dims;
                scalar.bits = bits;
                scalar.directoryType = directoryType;
                scalar.int4Encoding = int4Encoding;
                scalar.similarityFunction = similarityFunction;
                scalar.setup(data);

                float[] expected = scalar.score();

                vectorized.implementation = VectorScorerOSQBenchmark.VectorImplementation.VECTORIZED;
                vectorized.dims = dims;
                vectorized.bits = bits;
                vectorized.directoryType = directoryType;
                vectorized.int4Encoding = int4Encoding;
                vectorized.similarityFunction = similarityFunction;
                vectorized.setup(data);

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
        for (int i = 0; i < REPETITIONS; i++) {
            var seed = randomLong();

            var scalar = new VectorScorerOSQBenchmark();
            var vectorized = new VectorScorerOSQBenchmark();
            try {
                var data = VectorScorerOSQBenchmark.generateRandomVectorData(
                    new Random(seed),
                    dims,
                    bits,
                    int4Encoding,
                    similarityFunction
                );

                scalar.implementation = VectorScorerOSQBenchmark.VectorImplementation.SCALAR;
                scalar.dims = dims;
                scalar.bits = bits;
                scalar.directoryType = directoryType;
                scalar.int4Encoding = int4Encoding;
                scalar.similarityFunction = similarityFunction;
                scalar.setup(data);

                float[] expected = scalar.bulkScore();

                vectorized.implementation = VectorScorerOSQBenchmark.VectorImplementation.VECTORIZED;
                vectorized.dims = dims;
                vectorized.bits = bits;
                vectorized.directoryType = directoryType;
                vectorized.int4Encoding = int4Encoding;
                vectorized.similarityFunction = similarityFunction;
                vectorized.setup(data);

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
    public static Iterable<Object[]> parametersFactory() throws NoSuchFieldException {
        return generateParameters(
            VectorScorerOSQBenchmark.class.getField("dims"),
            VectorScorerOSQBenchmark.class.getField("bits"),
            VectorScorerOSQBenchmark.class.getField("directoryType"),
            VectorScorerOSQBenchmark.class.getField("int4Encoding"),
            VectorScorerOSQBenchmark.class.getField("similarityFunction")
        );
    }
}
