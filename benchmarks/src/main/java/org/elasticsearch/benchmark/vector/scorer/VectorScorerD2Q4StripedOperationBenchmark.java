/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.benchmark.vector.scorer;

import org.apache.lucene.store.Directory;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.nativeaccess.BBQTestUtils;
import org.elasticsearch.nativeaccess.NativeAccess;
import org.elasticsearch.nativeaccess.VectorSimilarityFunctions;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.rethrow;

/**
 * Kernel-direct benchmark for the 2-bit-doc / 4-bit-query striped (bit-plane) BBQ
 * dot product, dispatching straight to {@code vec_dotd2q4} via {@link VectorSimilarityFunctions}
 * and bypassing all Lucene/scorer infrastructure. Three modes are exercised on the same dataset:
 * <ul>
 *   <li>{@code scoreSingle} — single-pair calls in a sequential walk (control)</li>
 *   <li>{@code scoreBulk} — contiguous bulk slice</li>
 *   <li>{@code scoreBulkOffsets} — bulk via an offsets array (random access pattern)</li>
 * </ul>
 * Sparse is intentionally skipped: OSQ's {@code NativeMemorySegmentScorer} does not use it for D2Q4.
 * <p>
 * Run with: {@code ./gradlew -p benchmarks run --args 'VectorScorerD2Q4StripedOperationBenchmark'}
 */
@Fork(value = 1, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
public class VectorScorerD2Q4StripedOperationBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    @Param({ "128", "256", "512", "1024", "1536", "2048" })
    public int dims;

    // At dims=1024, packed doc length is 256 bytes.
    // 512 vectors = 128KB (overflows L1),
    // 10000 = 2.5MB (overflows L2),
    // 2000000 = ~488MB (overflows L3 on most cores).
    @Param({ "512", "10000", "2000000" })
    public int numVectors;

    @Param({ "32" })
    public int bulkSize;

    private Arena arena;

    // dataset: numVectors * docBytes laid out contiguously in native memory
    private MemorySegment dataset;
    // query: queryBytes (4 bit-planes back-to-back, produced by transposeHalfByte-style packing)
    private MemorySegment query;
    // shuffled ordinals for the random-access offsets path
    private int[] ordinals;
    private int numVectorsToScore;
    // doc bytes per vector = dims/4 (2 bit-planes, dims/8 bytes each). This is also the
    // length parameter passed to the kernel.
    private int docBytes;
    // native scratch for bulk_offsets ordinals and bulk results
    private MemorySegment ordinalsSeg;
    private MemorySegment resultsSeg;
    // Java-side results returned from each @Benchmark to suppress dead-code elimination
    private float[] scores;

    private MethodHandle singleImpl;
    private MethodHandle bulkImpl;
    private MethodHandle bulkOffsetsImpl;

    static final class VectorData extends VectorScorerBulkBenchmark.VectorData {
        private final byte[][] packedDocs;
        private final byte[] packedQuery;

        VectorData(int dims, int numVectors, int numVectorsToScore, Random random) {
            super(numVectors, numVectorsToScore, random);
            packedDocs = new byte[numVectors][];
            byte[] unpackedDoc = new byte[dims];
            for (int v = 0; v < numVectors; v++) {
                randomBytes(random, unpackedDoc, 4);
                packedDocs[v] = BBQTestUtils.packStriped(unpackedDoc, 2);
            }
            byte[] unpackedQuery = new byte[dims];
            randomBytes(random, unpackedQuery, 16);
            packedQuery = BBQTestUtils.packStriped(unpackedQuery, 4);
        }

        @Override
        void writeVectorData(Directory directory) throws IOException {
            // not directory-backed
        }
    }

    @Setup
    public void setup() {
        setup(new VectorData(dims, numVectors, Math.min(numVectors, 20_000), ThreadLocalRandom.current()));
    }

    void setup(VectorData vectorData) {
        assert dims % 8 == 0 : "D2Q4 striped requires dims divisible by 8: " + dims;
        arena = Arena.ofConfined();
        numVectorsToScore = vectorData.numVectorsToScore;
        docBytes = dims / 4;

        dataset = arena.allocate((long) numVectors * docBytes);
        for (int v = 0; v < numVectors; v++) {
            MemorySegment.copy(vectorData.packedDocs[v], 0, dataset, ValueLayout.JAVA_BYTE, (long) v * docBytes, docBytes);
        }

        int queryBytes = dims / 2;
        query = arena.allocate(queryBytes);
        MemorySegment.copy(vectorData.packedQuery, 0, query, ValueLayout.JAVA_BYTE, 0L, queryBytes);

        ordinals = vectorData.ordinals;

        ordinalsSeg = arena.allocate((long) bulkSize * Integer.BYTES);
        resultsSeg = arena.allocate((long) bulkSize * Float.BYTES);
        scores = new float[bulkSize];

        singleImpl = vectorSimilarityFunctions.getHandle(
            VectorSimilarityFunctions.Function.DOT_PRODUCT,
            VectorSimilarityFunctions.BBQType.D2Q4,
            VectorSimilarityFunctions.Operation.SINGLE
        );
        bulkImpl = vectorSimilarityFunctions.getHandle(
            VectorSimilarityFunctions.Function.DOT_PRODUCT,
            VectorSimilarityFunctions.BBQType.D2Q4,
            VectorSimilarityFunctions.Operation.BULK
        );
        bulkOffsetsImpl = vectorSimilarityFunctions.getHandle(
            VectorSimilarityFunctions.Function.DOT_PRODUCT,
            VectorSimilarityFunctions.BBQType.D2Q4,
            VectorSimilarityFunctions.Operation.BULK_OFFSETS
        );
    }

    @TearDown
    public void teardown() {
        arena.close();
    }

    /** Single-pair scoring, sequential walk (control vs scoreBulk). */
    @Benchmark
    public float[] scoreSingle() {
        try {
            int v = 0;
            while (v < numVectorsToScore) {
                for (int i = 0; i < bulkSize && v < numVectorsToScore; i++, v++) {
                    MemorySegment vec = dataset.asSlice((long) v * docBytes, docBytes);
                    scores[i] = (long) singleImpl.invokeExact(vec, query, docBytes);
                }
            }
        } catch (Throwable t) {
            throw rethrow(t);
        }
        return scores;
    }

    /** BULK: contiguous slice, sequential by construction. */
    @Benchmark
    public float[] scoreBulk() {
        try {
            for (int i = 0; i < numVectorsToScore; i += bulkSize) {
                int count = Math.min(bulkSize, numVectorsToScore - i);
                MemorySegment slice = dataset.asSlice((long) i * docBytes, (long) count * docBytes);
                bulkImpl.invokeExact(slice, query, docBytes, count, resultsSeg);
            }
        } catch (Throwable t) {
            throw rethrow(t);
        }
        MemorySegment.copy(resultsSeg, ValueLayout.JAVA_FLOAT, 0L, scores, 0, scores.length);
        return scores;
    }

    /** BULK_OFFSETS: scattered access driven by an int32 ordinals array. */
    @Benchmark
    public float[] scoreBulkOffsets() {
        try {
            for (int i = 0; i < numVectorsToScore; i += bulkSize) {
                int count = Math.min(bulkSize, numVectorsToScore - i);
                MemorySegment.copy(ordinals, i, ordinalsSeg, ValueLayout.JAVA_INT, 0L, count);
                bulkOffsetsImpl.invokeExact(dataset, query, docBytes, docBytes, ordinalsSeg, count, resultsSeg);
            }
        } catch (Throwable t) {
            throw rethrow(t);
        }
        MemorySegment.copy(resultsSeg, ValueLayout.JAVA_FLOAT, 0L, scores, 0, scores.length);
        return scores;
    }

    /**
     * Test-only helper: single-pair scoring using the same shuffled ordinals as
     * {@link #scoreBulkOffsets}, so the test can assert offsets correctness.
     */
    float[] scoreSingleAtOrdinals() {
        try {
            int v = 0;
            while (v < numVectorsToScore) {
                for (int i = 0; i < bulkSize && v < numVectorsToScore; i++, v++) {
                    MemorySegment vec = dataset.asSlice((long) ordinals[v] * docBytes, docBytes);
                    scores[i] = (long) singleImpl.invokeExact(vec, query, docBytes);
                }
            }
        } catch (Throwable t) {
            throw rethrow(t);
        }
        return scores;
    }

    private static void randomBytes(Random random, byte[] dst, int rangeExcl) {
        for (int i = 0; i < dst.length; i++) {
            dst[i] = (byte) random.nextInt(rangeExcl);
        }
    }

    private static final VectorSimilarityFunctions vectorSimilarityFunctions = NativeAccess.instance()
        .getVectorSimilarityFunctions()
        .orElseThrow();
}
