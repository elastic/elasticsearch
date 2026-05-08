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
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.simdvec.ES940OSQVectorsScorer;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.io.IOException;
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

    public void testSingle() throws Exception {
        for (int i = 0; i < REPETITIONS; i++) {
            var seed = randomLong();

            var data = VectorScorerOSQBenchmark.generateRandomVectorData(new Random(seed), dims, bits, int4Encoding, similarityFunction);

            float[] expected = null;
            for (var impl : VectorScorerOSQBenchmark.VectorImplementation.values()) {
                VectorScorerOSQBenchmark bench = new VectorScorerOSQBenchmark();
                bench.implementation = impl;
                bench.dims = dims;
                bench.bits = bits;
                bench.directoryType = directoryType;
                bench.int4Encoding = int4Encoding;
                bench.similarityFunction = similarityFunction;
                bench.setup(data);

                try {
                    float[] result = bench.controlScoreIndividual();
                    // just check against the first one - they should all be identical to each other
                    if (expected == null) {
                        expected = result;
                        continue;
                    }
                    assertArrayEqualsPercent(impl.toString(), expected, result, deltaPercent, DEFAULT_DELTA);
                } finally {
                    bench.teardown();
                    IOUtils.rm(bench.tempDir);
                }
            }
        }
    }

    public void testBulk() throws Exception {
        for (int i = 0; i < REPETITIONS; i++) {
            var seed = randomLong();

            var data = VectorScorerOSQBenchmark.generateRandomVectorData(new Random(seed), dims, bits, int4Encoding, similarityFunction);

            float[] expected = null;
            for (var impl : VectorScorerOSQBenchmark.VectorImplementation.values()) {
                VectorScorerOSQBenchmark bench = new VectorScorerOSQBenchmark();
                bench.implementation = impl;
                bench.dims = dims;
                bench.bits = bits;
                bench.directoryType = directoryType;
                bench.int4Encoding = int4Encoding;
                bench.similarityFunction = similarityFunction;
                bench.setup(data);

                try {
                    float[] result = bench.scoreBulk();
                    // just check against the first one - they should all be identical to each other
                    if (expected == null) {
                        expected = result;
                        continue;
                    }
                    assertArrayEqualsPercent(impl.toString(), expected, result, deltaPercent, DEFAULT_DELTA);
                } finally {
                    bench.teardown();
                    IOUtils.rm(bench.tempDir);
                }
            }
        }
    }

    public void testFilteredOne() throws Exception {
        for (int i = 0; i < REPETITIONS; i++) {
            var data = VectorScorerOSQBenchmark.generateRandomVectorData(
                new Random(randomLong()),
                dims,
                bits,
                int4Encoding,
                similarityFunction
            );
            runFilteredBenchmarks(
                data,
                VectorScorerOSQBenchmark::controlScoreBulkFilteredOne,
                VectorScorerOSQBenchmark::scoreIndividualFilteredOne,
                VectorScorerOSQBenchmark.SINGLE_OFFSET,
                1
            );
        }
    }

    public void testFilteredDense() throws Exception {
        for (int i = 0; i < REPETITIONS; i++) {
            var data = VectorScorerOSQBenchmark.generateRandomVectorData(
                new Random(randomLong()),
                dims,
                bits,
                int4Encoding,
                similarityFunction
            );
            runFilteredBenchmarks(
                data,
                VectorScorerOSQBenchmark::scoreBulkFilteredDense,
                VectorScorerOSQBenchmark::controlScoreIndividualFilteredDense,
                data.denseOffsets(),
                data.denseOffsetsCount()
            );
        }
    }

    public void testFilteredSparse() throws Exception {
        for (int i = 0; i < REPETITIONS; i++) {
            var data = VectorScorerOSQBenchmark.generateRandomVectorData(
                new Random(randomLong()),
                dims,
                bits,
                int4Encoding,
                similarityFunction
            );
            runFilteredBenchmarks(
                data,
                VectorScorerOSQBenchmark::scoreBulkFilteredSparse,
                VectorScorerOSQBenchmark::controlScoreIndividualFilteredSparse,
                data.sparseOffsets(),
                data.sparseOffsetsCount()
            );
        }
    }

    /**
     * Runs the bulk and per-vector variants of a filtered scoring benchmark across all {@link VectorScorerOSQBenchmark.VectorImplementation}s
     * on the same input data, asserting per-implementation that the two paths agree at the selected offsets within each
     * {@link VectorScorerOSQBenchmark#BULK_SIZE} chunk, and that all implementations agree with each other.
     *
     * <p>Non-selected scratch slots hold implementation-defined leftover values that differ between scoring paths and are intentionally
     * ignored.
     */
    private void runFilteredBenchmarks(
        VectorScorerOSQBenchmark.VectorData data,
        CheckedFunction<VectorScorerOSQBenchmark, float[], IOException> bulkBenchmark,
        CheckedFunction<VectorScorerOSQBenchmark, float[], IOException> individualBenchmark,
        int[] offsets,
        int offsetsCount
    ) throws Exception {
        var implementations = VectorScorerOSQBenchmark.VectorImplementation.values();
        float[] firstImplBulkResult = runIndividualVsBulk(
            data,
            implementations[0],
            bulkBenchmark,
            individualBenchmark,
            offsets,
            offsetsCount
        );
        for (int i = 1; i < implementations.length; i++) {
            float[] result = runIndividualVsBulk(data, implementations[i], bulkBenchmark, individualBenchmark, offsets, offsetsCount);
            assertEqualAtOffsets(implementations[i] + " cross-impl", firstImplBulkResult, result, offsets, offsetsCount);
        }
    }

    /**
     * Runs both benchmark variants once for the given implementation, asserts they agree at the selected offsets, and returns the bulk
     * result for cross-implementation comparison by the caller.
     */
    private float[] runIndividualVsBulk(
        VectorScorerOSQBenchmark.VectorData data,
        VectorScorerOSQBenchmark.VectorImplementation impl,
        CheckedFunction<VectorScorerOSQBenchmark, float[], IOException> bulkBenchmark,
        CheckedFunction<VectorScorerOSQBenchmark, float[], IOException> individualBenchmark,
        int[] offsets,
        int offsetsCount
    ) throws Exception {
        VectorScorerOSQBenchmark bench = new VectorScorerOSQBenchmark();
        bench.implementation = impl;
        bench.dims = dims;
        bench.bits = bits;
        bench.directoryType = directoryType;
        bench.int4Encoding = int4Encoding;
        bench.similarityFunction = similarityFunction;
        bench.setup(data);

        try {
            float[] bulkResult = bulkBenchmark.apply(bench);
            float[] individualResult = individualBenchmark.apply(bench);
            assertEqualAtOffsets(impl + " bulk vs individual", bulkResult, individualResult, offsets, offsetsCount);
            return bulkResult;
        } finally {
            bench.teardown();
            IOUtils.rm(bench.tempDir);
        }
    }

    private void assertEqualAtOffsets(String message, float[] expected, float[] actual, int[] offsets, int offsetsCount) {
        assertEquals(message + " length", expected.length, actual.length);
        for (int chunkStart = 0; chunkStart < expected.length; chunkStart += VectorScorerOSQBenchmark.BULK_SIZE) {
            for (int o = 0; o < offsetsCount; o++) {
                int idx = chunkStart + offsets[o];
                float delta = Math.max(Math.abs(expected[idx] * deltaPercent), DEFAULT_DELTA);
                assertEquals(message + " at " + idx, expected[idx], actual[idx], delta);
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
