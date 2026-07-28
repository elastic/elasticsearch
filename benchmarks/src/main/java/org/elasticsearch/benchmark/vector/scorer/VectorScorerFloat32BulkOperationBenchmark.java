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
import org.elasticsearch.index.codec.vectors.VectorTestUtils;
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
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

/**
 * Bare-bones bulk operation benchmark for float32 vector similarity functions.
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
 * Run with: {@code ./gradlew -p benchmarks run --args 'VectorScorerFloat32BulkOperationBenchmark'}
 */
@Fork(value = 1, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
public class VectorScorerFloat32BulkOperationBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    @Param({ "1024" })
    public int dims;

    // With dims=1024, each vector is 4KB. Target cache overflow points:
    // c8a (AMD EPYC): L1=48KB, L2=1MB, L3=16MB
    // c8g (Graviton 4): L1=64KB, L2=2MB, L3=36MB
    // 32 vectors = 128KB: overflows L1 on both, fits in L2
    // 375 vectors = 1.5MB: overflows L2 on AMD, fits in L3 on AMD; fits in L2 on Graviton
    // 32500 vectors = ~127MB: overflows L3 on both
    @Param({ "32", "375", "32500" })
    public int numVectors;

    @Param({ "32", "64", "256", "1024" })
    public int bulkSize;

    @Param({ "DOT_PRODUCT", "EUCLIDEAN" })
    public VectorSimilarityType function;

    private Arena arena;

    // Dataset: numVectors vectors laid out contiguously in native memory, each `dims * Float.BYTES` bytes.
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

    static final class VectorData extends VectorScorerBulkBenchmark.VectorData {
        private final float[][] vectors;

        VectorData(int dims, int numVectors, int numVectorsToScore, Random random) {
            super(numVectors, numVectorsToScore, random);

            vectors = new float[numVectors][];
            for (int v = 0; v < numVectors; v++) {
                vectors[v] = VectorTestUtils.randomFloatVector(random, dims);
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
        dataset = arena.allocate((long) numVectors * dims * Float.BYTES);
        for (int v = 0; v < numVectors; v++) {
            MemorySegment.copy(vectorData.vectors[v], 0, dataset, ValueLayout.JAVA_FLOAT, (long) v * dims, dims);
        }
        datasetAddress = dataset.address();

        // Query vector: use the target ordinal's vector
        query = arena.allocate((long) dims * Float.BYTES);
        MemorySegment.copy(vectorData.vectors[vectorData.targetOrd], 0, query, ValueLayout.JAVA_FLOAT, 0L, dims);

        // Sequential and random ordinals
        ids = IntStream.range(0, numVectors).toArray();
        ordinals = vectorData.ordinals;

        // Native scratch buffers for bulk calls
        ordinalsSeg = arena.allocate((long) bulkSize * Integer.BYTES);
        addressesSeg = arena.allocate((long) bulkSize * Long.BYTES);
        resultsSeg = arena.allocate((long) bulkSize * Float.BYTES);
        scores = new float[bulkSize];

    }

    @TearDown
    public void teardown() {
        arena.close();
    }

    private float callSingleScore(MemorySegment vec, MemorySegment query, int dims) {
        return switch (function) {
            case DOT_PRODUCT -> vectorSimilarityFunctions.dotProductF32(vec, query, dims);
            case EUCLIDEAN -> vectorSimilarityFunctions.squareDistanceF32(vec, query, dims);
            default -> throw new UnsupportedOperationException(function.toString());
        };
    }

    private void callBulkScore(MemorySegment a, MemorySegment b, int dims, int count, MemorySegment results) {
        switch (function) {
            case DOT_PRODUCT -> vectorSimilarityFunctions.dotProductF32Bulk(a, b, dims, count, results);
            case EUCLIDEAN -> vectorSimilarityFunctions.squareDistanceF32Bulk(a, b, dims, count, results);
            default -> throw new UnsupportedOperationException(function.toString());
        }
    }

    private void callBulkOffsetsScore(
        MemorySegment a,
        MemorySegment b,
        int dims,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment results
    ) {
        switch (function) {
            case DOT_PRODUCT -> vectorSimilarityFunctions.dotProductF32BulkWithOffsets(a, b, dims, pitch, offsets, count, results);
            case EUCLIDEAN -> vectorSimilarityFunctions.squareDistanceF32BulkWithOffsets(a, b, dims, pitch, offsets, count, results);
            default -> throw new UnsupportedOperationException(function.toString());
        }
    }

    private void callBulkSparseScore(MemorySegment addresses, MemorySegment b, int dims, int count, MemorySegment results) {
        switch (function) {
            case DOT_PRODUCT -> vectorSimilarityFunctions.dotProductF32BulkSparse(addresses, b, dims, count, results);
            case EUCLIDEAN -> vectorSimilarityFunctions.squareDistanceF32BulkSparse(addresses, b, dims, count, results);
            default -> throw new UnsupportedOperationException(function.toString());
        }
    }

    /** Single-pair scoring, sequential ids (control). */
    @Benchmark
    public float[] scoreSequential() {
        int v = 0;
        long vecBytes = (long) dims * Float.BYTES;
        while (v < numVectorsToScore) {
            for (int i = 0; i < bulkSize && v < numVectorsToScore; i++, v++) {
                MemorySegment vec = dataset.asSlice((long) ids[v] * vecBytes, vecBytes);
                scores[i] = callSingleScore(vec, query, dims);
            }
        }
        return scores;
    }

    /** Single-pair scoring, shuffled ordinals (control). */
    @Benchmark
    public float[] scoreRandom() {
        int v = 0;
        long vecBytes = (long) dims * Float.BYTES;
        while (v < numVectorsToScore) {
            for (int i = 0; i < bulkSize && v < numVectorsToScore; i++, v++) {
                MemorySegment vec = dataset.asSlice((long) ordinals[v] * vecBytes, vecBytes);
                scores[i] = callSingleScore(vec, query, dims);
            }
        }
        return scores;
    }

    /** BULK: contiguous slice — sequential by construction. */
    @Benchmark
    public float[] scoreBulk() {
        long vecBytes = (long) dims * Float.BYTES;
        for (int i = 0; i < numVectorsToScore; i += bulkSize) {
            int count = Math.min(bulkSize, numVectorsToScore - i);
            MemorySegment slice = dataset.asSlice((long) i * vecBytes, (long) count * vecBytes);
            callBulkScore(slice, query, dims, count, resultsSeg);
        }
        MemorySegment.copy(resultsSeg, ValueLayout.JAVA_FLOAT, 0L, scores, 0, scores.length);
        return scores;
    }

    /** BULK_OFFSETS: scattered access driven by an int32 ordinals array. */
    @Benchmark
    public float[] scoreBulkOffsets() {
        int stride = dims * Float.BYTES;
        for (int i = 0; i < numVectorsToScore; i += bulkSize) {
            int count = Math.min(bulkSize, numVectorsToScore - i);
            MemorySegment.copy(ordinals, i, ordinalsSeg, ValueLayout.JAVA_INT, 0L, count);
            callBulkOffsetsScore(dataset, query, dims, stride, ordinalsSeg, count, resultsSeg);
        }
        MemorySegment.copy(resultsSeg, ValueLayout.JAVA_FLOAT, 0L, scores, 0, scores.length);
        return scores;
    }

    /** BULK_SPARSE: scattered access driven by a pre-resolved address array. */
    @Benchmark
    public float[] scoreBulkSparse() {
        long vecBytes = (long) dims * Float.BYTES;
        for (int i = 0; i < numVectorsToScore; i += bulkSize) {
            int count = Math.min(bulkSize, numVectorsToScore - i);
            for (int j = 0; j < count; j++) {
                long addr = datasetAddress + (long) ordinals[i + j] * vecBytes;
                addressesSeg.set(ValueLayout.JAVA_LONG, (long) j * Long.BYTES, addr);
            }
            callBulkSparseScore(addressesSeg, query, dims, count, resultsSeg);
        }
        MemorySegment.copy(resultsSeg, ValueLayout.JAVA_FLOAT, 0L, scores, 0, scores.length);
        return scores;
    }

    private static final VectorSimilarityFunctions vectorSimilarityFunctions = NativeAccess.instance()
        .getVectorSimilarityFunctions()
        .orElseThrow();
}
