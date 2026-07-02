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
import java.util.stream.IntStream;

import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.rethrow;
import static org.elasticsearch.nativeaccess.Int4TestUtils.packNibbles;
import static org.elasticsearch.simdvec.internal.vectorization.VectorScorerTestUtils.randomInt4Bytes;

/**
 * Bare-bones bulk operation benchmark for int4 packed-nibble vector dot product.
 * Dispatches directly to the native BULK / BULK_OFFSETS / BULK_SPARSE implementations
 * via {@link VectorSimilarityFunctions}, bypassing the Lucene scorer / corrective-terms
 * infrastructure so the inner SIMD kernel cost is the dominant signal:
 * <ul>
 *   <li>{@code scoreBulk} — contiguous slice (sequential by construction)</li>
 *   <li>{@code scoreBulkOffsets} — scattered access via int32 offsets array</li>
 *   <li>{@code scoreBulkSparse} — scattered access via pre-resolved address array</li>
 * </ul>
 * {@code scoreSequential} and {@code scoreRandom} are single-pair controls.
 * <p>
 * Run with: {@code ./gradlew -p benchmarks run --args 'VectorScorerInt4BulkOperationBenchmark'}
 */
@Fork(value = 1, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
public class VectorScorerInt4BulkOperationBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    @Param({ "1024" })
    public int dims;

    // dims=1024 -> packed_len = 512 bytes per vector. Cache overflow points:
    // c8a (Zen 5): L1=48KB, L2=1MB, L3=16MB
    // c8i (SPR/GR): L1=48KB, L2=2MB, L3>=8MB
    // 128 vectors = 64KB: overflows L1 on Zen 5, fits in L2
    // 2500 vectors = 1.25MB: overflows L2 on Zen 5, fits in L3
    // 130000 vectors = ~63MB: overflows L3 on both
    @Param({ "128", "2500", "130000" })
    public int numVectors;

    @Param({ "32", "64", "256", "1024" })
    public int bulkSize;

    private Arena arena;

    // packed dataset: numVectors * (dims/2) bytes laid out contiguously in native memory
    private MemorySegment dataset;
    // unpacked query: dims bytes (high nibbles in [0..dims/2), low nibbles in [dims/2..dims))
    private MemorySegment query;
    // sequential ids and shuffled ordinals
    private int[] ids;
    private int[] ordinals;
    private int numVectorsToScore;
    private int packedLen;
    private long datasetAddress;
    // native scratch for bulk_offsets (int32 ordinals) and bulk_sparse (int64 addresses)
    private MemorySegment ordinalsSeg;
    private MemorySegment addressesSeg;
    private MemorySegment resultsSeg;
    // Java-side results, returned to prevent dead-code elimination
    private float[] scores;

    private MethodHandle singleImpl;
    private MethodHandle bulkImpl;
    private MethodHandle bulkOffsetsImpl;
    private MethodHandle bulkSparseImpl;

    static final class VectorData extends VectorScorerBulkBenchmark.VectorData {
        private final byte[][] packedVectors;
        private final byte[] queryUnpacked;

        VectorData(int dims, int numVectors, int numVectorsToScore, Random random) {
            super(numVectors, numVectorsToScore, random);
            packedVectors = new byte[numVectors][];
            for (int v = 0; v < numVectors; v++) {
                byte[] unpacked = new byte[dims];
                randomInt4Bytes(random, unpacked);
                packedVectors[v] = packNibbles(unpacked);
            }
            queryUnpacked = new byte[dims];
            randomInt4Bytes(random, queryUnpacked);
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
        arena = Arena.ofConfined();
        numVectorsToScore = vectorData.numVectorsToScore;
        packedLen = dims / 2;

        dataset = arena.allocate((long) numVectors * packedLen);
        for (int v = 0; v < numVectors; v++) {
            MemorySegment.copy(vectorData.packedVectors[v], 0, dataset, ValueLayout.JAVA_BYTE, (long) v * packedLen, packedLen);
        }
        datasetAddress = dataset.address();

        query = arena.allocate(dims);
        MemorySegment.copy(vectorData.queryUnpacked, 0, query, ValueLayout.JAVA_BYTE, 0L, dims);

        ids = IntStream.range(0, numVectors).toArray();
        ordinals = vectorData.ordinals;

        ordinalsSeg = arena.allocate((long) bulkSize * Integer.BYTES);
        addressesSeg = arena.allocate((long) bulkSize * Long.BYTES);
        resultsSeg = arena.allocate((long) bulkSize * Float.BYTES);
        scores = new float[bulkSize];

        singleImpl = vectorSimilarityFunctions.getHandle(
            VectorSimilarityFunctions.Function.DOT_PRODUCT,
            VectorSimilarityFunctions.DataType.INT4,
            VectorSimilarityFunctions.Operation.SINGLE
        );
        bulkImpl = vectorSimilarityFunctions.getHandle(
            VectorSimilarityFunctions.Function.DOT_PRODUCT,
            VectorSimilarityFunctions.DataType.INT4,
            VectorSimilarityFunctions.Operation.BULK
        );
        bulkOffsetsImpl = vectorSimilarityFunctions.getHandle(
            VectorSimilarityFunctions.Function.DOT_PRODUCT,
            VectorSimilarityFunctions.DataType.INT4,
            VectorSimilarityFunctions.Operation.BULK_OFFSETS
        );
        bulkSparseImpl = vectorSimilarityFunctions.getHandle(
            VectorSimilarityFunctions.Function.DOT_PRODUCT,
            VectorSimilarityFunctions.DataType.INT4,
            VectorSimilarityFunctions.Operation.BULK_SPARSE
        );
    }

    @TearDown
    public void teardown() {
        arena.close();
    }

    /** Single-pair scoring, sequential ids (control). */
    @Benchmark
    public float[] scoreSequential() {
        try {
            int v = 0;
            while (v < numVectorsToScore) {
                for (int i = 0; i < bulkSize && v < numVectorsToScore; i++, v++) {
                    MemorySegment vec = dataset.asSlice((long) ids[v] * packedLen, packedLen);
                    scores[i] = (int) singleImpl.invokeExact(query, vec, packedLen);
                }
            }
        } catch (Throwable t) {
            throw rethrow(t);
        }
        return scores;
    }

    /** Single-pair scoring, shuffled ordinals (control). */
    @Benchmark
    public float[] scoreRandom() {
        try {
            int v = 0;
            while (v < numVectorsToScore) {
                for (int i = 0; i < bulkSize && v < numVectorsToScore; i++, v++) {
                    MemorySegment vec = dataset.asSlice((long) ordinals[v] * packedLen, packedLen);
                    scores[i] = (int) singleImpl.invokeExact(query, vec, packedLen);
                }
            }
        } catch (Throwable t) {
            throw rethrow(t);
        }
        return scores;
    }

    /** BULK: contiguous slice — sequential by construction. */
    @Benchmark
    public float[] scoreBulk() {
        try {
            for (int i = 0; i < numVectorsToScore; i += bulkSize) {
                int count = Math.min(bulkSize, numVectorsToScore - i);
                MemorySegment slice = dataset.asSlice((long) i * packedLen, (long) count * packedLen);
                bulkImpl.invokeExact(slice, query, packedLen, count, resultsSeg);
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
                bulkOffsetsImpl.invokeExact(dataset, query, packedLen, packedLen, ordinalsSeg, count, resultsSeg);
            }
        } catch (Throwable t) {
            throw rethrow(t);
        }
        MemorySegment.copy(resultsSeg, ValueLayout.JAVA_FLOAT, 0L, scores, 0, scores.length);
        return scores;
    }

    /** BULK_SPARSE: scattered access driven by a pre-resolved address array. */
    @Benchmark
    public float[] scoreBulkSparse() {
        try {
            for (int i = 0; i < numVectorsToScore; i += bulkSize) {
                int count = Math.min(bulkSize, numVectorsToScore - i);
                for (int j = 0; j < count; j++) {
                    long addr = datasetAddress + (long) ordinals[i + j] * packedLen;
                    addressesSeg.set(ValueLayout.JAVA_LONG, (long) j * Long.BYTES, addr);
                }
                bulkSparseImpl.invokeExact(addressesSeg, query, packedLen, count, resultsSeg);
            }
        } catch (Throwable t) {
            throw rethrow(t);
        }
        MemorySegment.copy(resultsSeg, ValueLayout.JAVA_FLOAT, 0L, scores, 0, scores.length);
        return scores;
    }

    private static final VectorSimilarityFunctions vectorSimilarityFunctions = NativeAccess.instance()
        .getVectorSimilarityFunctions()
        .orElseThrow();
}
