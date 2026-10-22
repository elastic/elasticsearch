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
import org.elasticsearch.benchmark.store.DirectoryType;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.simdvec.BulkMask;
import org.elasticsearch.simdvec.ES940OSQVectorsScorer.QuantEncoding;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.io.IOException;

@TestLogging(
    reason = "Noisy logging",
    value = "org.elasticsearch.env.NodeEnvironment:WARN,org.elasticsearch.xpack.searchablesnapshots.cache.full.PersistentCache:WARN"
)
public class VectorScorerOSQBenchmarkTests extends BenchmarkTest {

    private final float deltaPercent = 0.1f;
    private final int dims;
    private final QuantEncoding quantConfig;
    private final DirectoryType directoryType;
    private final VectorSimilarityFunction similarityFunction;

    public VectorScorerOSQBenchmarkTests(
        int dims,
        QuantEncoding quantConfig,
        DirectoryType directoryType,
        VectorSimilarityFunction similarityFunction
    ) {
        this.dims = dims;
        this.quantConfig = quantConfig;
        this.directoryType = directoryType;
        this.similarityFunction = similarityFunction;
    }

    public void testSingle() throws Exception {
        var data = VectorScorerOSQBenchmark.generateRandomVectorData(random(), dims, quantConfig, similarityFunction);

        float[] expected = null;
        for (var impl : VectorScorerOSQBenchmark.VectorImplementation.values()) {
            VectorScorerOSQBenchmark bench = new VectorScorerOSQBenchmark();
            bench.implementation = impl;
            bench.dims = dims;
            bench.quantConfig = quantConfig;
            bench.directoryType = directoryType;
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
            }
        }
    }

    public void testBulk() throws Exception {
        var data = VectorScorerOSQBenchmark.generateRandomVectorData(random(), dims, quantConfig, similarityFunction);

        float[] expected = null;
        for (var impl : VectorScorerOSQBenchmark.VectorImplementation.values()) {
            VectorScorerOSQBenchmark bench = new VectorScorerOSQBenchmark();
            bench.implementation = impl;
            bench.dims = dims;
            bench.quantConfig = quantConfig;
            bench.directoryType = directoryType;
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
            }
        }
    }

    public void testFilteredOne() throws Exception {
        var data = VectorScorerOSQBenchmark.generateRandomVectorData(random(), dims, quantConfig, similarityFunction);
        runFilteredBenchmarks(
            data,
            VectorScorerOSQBenchmark::controlScoreBulkFilteredOne,
            VectorScorerOSQBenchmark::scoreIndividualFilteredOne,
            VectorScorerOSQBenchmark.SINGLE_MASK
        );
    }

    public void testFilteredDense() throws Exception {
        var data = VectorScorerOSQBenchmark.generateRandomVectorData(random(), dims, quantConfig, similarityFunction);
        runFilteredBenchmarks(
            data,
            VectorScorerOSQBenchmark::scoreBulkFilteredDense,
            VectorScorerOSQBenchmark::controlScoreIndividualFilteredDense,
            data.denseMask()
        );
    }

    public void testFilteredSparse() throws Exception {
        var data = VectorScorerOSQBenchmark.generateRandomVectorData(random(), dims, quantConfig, similarityFunction);
        runFilteredBenchmarks(
            data,
            VectorScorerOSQBenchmark::scoreBulkFilteredSparse,
            VectorScorerOSQBenchmark::controlScoreIndividualFilteredSparse,
            data.sparseMask()
        );
    }

    /**
     * Runs the bulk and per-vector variants of a filtered scoring benchmark across all
     * {@link VectorScorerOSQBenchmark.VectorImplementation}s on the same input data, asserting per-implementation that the two paths agree
     * at the positions selected by {@code mask} within each {@link VectorScorerOSQBenchmark#BULK_SIZE} chunk, and that all
     * implementations agree.
     *
     * <p>Non-selected scratch slots hold implementation-defined leftover values that differ between scoring paths and are intentionally
     * ignored.
     */
    private void runFilteredBenchmarks(
        VectorScorerOSQBenchmark.VectorData data,
        CheckedFunction<VectorScorerOSQBenchmark, float[], IOException> bulkBenchmark,
        CheckedFunction<VectorScorerOSQBenchmark, float[], IOException> individualBenchmark,
        int mask
    ) throws Exception {
        var implementations = VectorScorerOSQBenchmark.VectorImplementation.values();
        float[] firstImplBulkResult = runIndividualVsBulk(data, implementations[0], bulkBenchmark, individualBenchmark, mask);
        for (int i = 1; i < implementations.length; i++) {
            float[] result = runIndividualVsBulk(data, implementations[i], bulkBenchmark, individualBenchmark, mask);
            assertEqualAtSelected(implementations[i] + " cross-impl", firstImplBulkResult, result, mask);
        }
    }

    /**
     * Runs both benchmark variants once for the given implementation, asserts they agree at the selected positions, and returns the
     * bulk result for cross-implementation comparison by the caller.
     */
    private float[] runIndividualVsBulk(
        VectorScorerOSQBenchmark.VectorData data,
        VectorScorerOSQBenchmark.VectorImplementation impl,
        CheckedFunction<VectorScorerOSQBenchmark, float[], IOException> bulkBenchmark,
        CheckedFunction<VectorScorerOSQBenchmark, float[], IOException> individualBenchmark,
        int mask
    ) throws Exception {
        VectorScorerOSQBenchmark bench = new VectorScorerOSQBenchmark();
        bench.implementation = impl;
        bench.dims = dims;
        bench.quantConfig = quantConfig;
        bench.directoryType = directoryType;
        bench.similarityFunction = similarityFunction;
        bench.setup(data);

        try {
            float[] bulkResult = bulkBenchmark.apply(bench);
            float[] individualResult = individualBenchmark.apply(bench);
            assertEqualAtSelected(impl + " bulk vs individual", bulkResult, individualResult, mask);
            return bulkResult;
        } finally {
            bench.teardown();
        }
    }

    private void assertEqualAtSelected(String message, float[] expected, float[] actual, int mask) {
        assertEquals(message + " length", expected.length, actual.length);
        for (int chunkStart = 0; chunkStart < expected.length; chunkStart += VectorScorerOSQBenchmark.BULK_SIZE) {
            for (int m = mask; m != 0; m = BulkMask.deselectFirst(m)) {
                int idx = chunkStart + BulkMask.first(m);
                float delta = Math.max(Math.abs(expected[idx] * deltaPercent), DEFAULT_DELTA);
                assertEquals(message + " at " + idx, expected[idx], actual[idx], delta);
            }
        }
    }

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() throws NoSuchFieldException {
        return generateParameters(
            VectorScorerOSQBenchmark.class.getField("dims"),
            VectorScorerOSQBenchmark.class.getField("quantConfig"),
            VectorScorerOSQBenchmark.class.getField("directoryType"),
            VectorScorerOSQBenchmark.class.getField("similarityFunction")
        );
    }
}
