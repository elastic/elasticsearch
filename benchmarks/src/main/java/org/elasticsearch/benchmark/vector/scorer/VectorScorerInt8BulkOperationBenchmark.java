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
import org.elasticsearch.simdvec.VectorSimilarityType;
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

/**
 * Bare-bones bulk operation benchmark for int8 vector similarity functions.
 * Dispatches directly to the native BULK / BULK_OFFSETS / BULK_SPARSE implementations
 * via {@link VectorSimilarityFunctions}, bypassing the Lucene scorer infrastructure
 * so the inner SIMD kernel cost is the dominant signal:
 * <ul>
 *   <li>{@code scoreBulk} — contiguous slice (sequential by construction)</li>
 *   <li>{@code scoreBulkOffsets} — scattered access via int32 offsets array</li>
 *   <li>{@code scoreBulkSparse} — scattered access via pre-resolved address array</li>
 * </ul>
 * {@code scoreSequential} and {@code scoreRandom} are single-pair controls.
 * <p>
 * Run with: {@code ./gradlew -p benchmarks run --args 'VectorScorerInt8BulkOperationBenchmark'}
 */
@Fork(value = 1, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
public class VectorScorerInt8BulkOperationBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    @Param({ "1024" })
    public int dims;

    // With dims=1024, each vector is 1KB. Target cache overflow points:
    // c8a (AMD EPYC): L1=48KB, L2=1MB, L3=16MB
    // c8g (Graviton 4): L1=64KB, L2=2MB, L3=36MB
    // 128 vectors = 128KB: overflows L1 on both, fits in L2
    // 2500 vectors = 2.5MB: overflows L2 on both, fits in L3
    // 130000 vectors = ~127MB: overflows L3 on both
    @Param({ "128", "2500", "130000" })
    public int numVectors;

    @Param({ "32", "64", "256", "1024" })
    public int bulkSize;

    @Param({ "COSINE", "DOT_PRODUCT", "EUCLIDEAN" })
    public VectorSimilarityType function;

    private Arena arena;

    // Dataset: numVectors vectors laid out contiguously in native memory, each `dims * Byte.BYTES` bytes.
    private MemorySegment dataset;
    // Query vector in native memory.
    private MemorySegment query;
    // Sequential ids [0, 1, 2, ...] and shuffled random ordinals.
    private int[] ids;
    private int[] ordinals;
    private int numVectorsToScore;
    private long datasetAddress;
    // Scratch buffers in native memory for bulk calls.
    private MemorySegment ordinalsSeg;
    private MemorySegment addressesSeg;
    private MemorySegment resultsSeg;
    // Java-side results array, returned from benchmarks to prevent dead-code elimination.
    private float[] scores;

    private MethodHandle singleImpl;
    private MethodHandle bulkImpl;
    private MethodHandle bulkOffsetsImpl;
    private MethodHandle bulkSparseImpl;

    // although this is not a directory-based BulkBenchmark, we can still use some bits in the VectorData impl
    static final class VectorData extends VectorScorerBulkBenchmark.VectorData {
        private final byte[][] vectors;

        VectorData(int dims, int numVectors, int numVectorsToScore, Random random) {
            super(numVectors, numVectorsToScore, random);

            vectors = new byte[numVectors][];
            for (int v = 0; v < numVectors; v++) {
                vectors[v] = new byte[dims];
                random.nextBytes(vectors[v]);
            }
        }

        @Override
        void writeVectorData(Directory directory) throws IOException {
            // doesn't use directories
        }
    }

    @Setup
    public void setup() {
        setup(new VectorData(dims, numVectors, Math.min(numVectors, 20_000), ThreadLocalRandom.current()));
    }

    void setup(VectorData vectorData) {
        arena = Arena.ofConfined();

        numVectorsToScore = vectorData.numVectorsToScore;

        // Allocate contiguous dataset in native memory
        dataset = arena.allocate((long) numVectors * dims);
        for (int v = 0; v < numVectors; v++) {
            MemorySegment.copy(vectorData.vectors[v], 0, dataset, ValueLayout.JAVA_BYTE, (long) v * dims, dims);
        }
        datasetAddress = dataset.address();

        // Query vector: use the target ordinal's vector
        query = arena.allocate(dims);
        MemorySegment.copy(vectorData.vectors[vectorData.targetOrd], 0, query, ValueLayout.JAVA_BYTE, 0L, dims);

        // Sequential and random ordinals
        ids = IntStream.range(0, numVectors).toArray();
        ordinals = vectorData.ordinals;

        // Native scratch buffers for bulk calls
        ordinalsSeg = arena.allocate((long) bulkSize * Integer.BYTES);
        addressesSeg = arena.allocate((long) bulkSize * Long.BYTES);
        resultsSeg = arena.allocate((long) bulkSize * Float.BYTES);
        scores = new float[bulkSize];

        VectorSimilarityFunctions.Function nativeFunc = switch (function) {
            case COSINE -> VectorSimilarityFunctions.Function.COSINE;
            case DOT_PRODUCT -> VectorSimilarityFunctions.Function.DOT_PRODUCT;
            case EUCLIDEAN -> VectorSimilarityFunctions.Function.SQUARE_DISTANCE;
            default -> throw new IllegalArgumentException(function.toString());
        };
        singleImpl = vectorSimilarityFunctions.getHandle(
            nativeFunc,
            VectorSimilarityFunctions.DataType.INT8,
            VectorSimilarityFunctions.Operation.SINGLE
        );
        bulkImpl = vectorSimilarityFunctions.getHandle(
            nativeFunc,
            VectorSimilarityFunctions.DataType.INT8,
            VectorSimilarityFunctions.Operation.BULK
        );
        bulkOffsetsImpl = vectorSimilarityFunctions.getHandle(
            nativeFunc,
            VectorSimilarityFunctions.DataType.INT8,
            VectorSimilarityFunctions.Operation.BULK_OFFSETS
        );
        bulkSparseImpl = vectorSimilarityFunctions.getHandle(
            nativeFunc,
            VectorSimilarityFunctions.DataType.INT8,
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
        int v = 0;
        try {
            while (v < numVectorsToScore) {
                for (int i = 0; i < bulkSize && v < numVectorsToScore; i++, v++) {
                    MemorySegment vec = dataset.asSlice((long) ids[v] * dims, dims);
                    scores[i] = (float) singleImpl.invokeExact(vec, query, dims);
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
        int v = 0;
        try {
            while (v < numVectorsToScore) {
                for (int i = 0; i < bulkSize && v < numVectorsToScore; i++, v++) {
                    MemorySegment vec = dataset.asSlice((long) ordinals[v] * dims, dims);
                    scores[i] = (float) singleImpl.invokeExact(vec, query, dims);
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
                MemorySegment slice = dataset.asSlice((long) i * dims, (long) count * dims);
                bulkImpl.invokeExact(slice, query, dims, count, resultsSeg);
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
                bulkOffsetsImpl.invokeExact(dataset, query, dims, dims, ordinalsSeg, count, resultsSeg);
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
                    long addr = datasetAddress + (long) ordinals[i + j] * dims;
                    addressesSeg.set(ValueLayout.JAVA_LONG, (long) j * Long.BYTES, addr);
                }
                bulkSparseImpl.invokeExact(addressesSeg, query, dims, count, resultsSeg);
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
