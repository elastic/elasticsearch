/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.benchmark.vector.scorer;

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

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.rethrow;

/**
 * Bare-bones bulk operation benchmark for int8 vector similarity functions.
 * Calls the native bulk_offsets operation directly via VectorSimilarityFunctions,
 * bypassing Lucene scorer infrastructure.
 * <p>
 * Run with: {@code ./gradlew -p benchmarks run --args 'VectorScorerInt8BulkOperationBenchmark'}
 */
@Fork(value = 1, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
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
    // Scratch buffers in native memory for bulk calls.
    private MemorySegment ordinalsSeg;
    private MemorySegment resultsSeg;
    // Java-side results array, returned from benchmarks to prevent dead-code elimination.
    private float[] scores;

    private MethodHandle singleImpl;
    private MethodHandle bulkOffsetsImpl;

    record VectorData(int numVectorsToScore, byte[][] vectors, int[] ordinals, int targetOrd) {

        static VectorData create(int dims, int numVectors, int numVectorsToScore, Random random) {
            var vectors = new byte[numVectors][];

            for (int v = 0; v < numVectors; v++) {
                vectors[v] = new byte[dims];
                random.nextBytes(vectors[v]);
            }

            List<Integer> list = IntStream.range(0, numVectors).boxed().collect(Collectors.toList());
            Collections.shuffle(list, random);
            var ordinals = list.stream().limit(numVectorsToScore).mapToInt(Integer::intValue).toArray();
            var targetOrd = random.nextInt(numVectors);

            return new VectorData(numVectorsToScore, vectors, ordinals, targetOrd);
        }
    }

    @Setup
    public void setup() {
        setup(VectorData.create(dims, numVectors, Math.min(numVectors, 20_000), ThreadLocalRandom.current()));
    }

    void setup(VectorData vectorData) {
        arena = Arena.ofConfined();

        numVectorsToScore = vectorData.numVectorsToScore;

        // Allocate contiguous dataset in native memory
        dataset = arena.allocate((long) numVectors * dims);
        for (int v = 0; v < numVectors; v++) {
            MemorySegment.copy(vectorData.vectors[v], 0, dataset, ValueLayout.JAVA_BYTE, (long) v * dims, dims);
        }

        // Query vector: use the target ordinal's vector
        query = arena.allocate(dims);
        MemorySegment.copy(vectorData.vectors[vectorData.targetOrd], 0, query, ValueLayout.JAVA_BYTE, 0L, dims);

        // Sequential and random ordinals
        ids = IntStream.range(0, numVectors).toArray();
        ordinals = vectorData.ordinals;

        // Native scratch buffers for bulk calls
        ordinalsSeg = arena.allocate((long) bulkSize * Integer.BYTES);
        resultsSeg = arena.allocate((long) bulkSize * Float.BYTES);
        scores = new float[bulkSize];

        // Get bulk_offsets method handle
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
        bulkOffsetsImpl = vectorSimilarityFunctions.getHandle(
            nativeFunc,
            VectorSimilarityFunctions.DataType.INT8,
            VectorSimilarityFunctions.Operation.BULK_OFFSETS
        );
    }

    @TearDown
    public void teardown() {
        arena.close();
    }

    @Benchmark
    public float[] scoreMultipleSequential() {
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

    @Benchmark
    public float[] scoreMultipleRandom() {
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

    @Benchmark
    public float[] scoreMultipleSequentialBulk() {
        try {
            for (int i = 0; i < numVectorsToScore; i += bulkSize) {
                int count = Math.min(bulkSize, numVectorsToScore - i);
                MemorySegment.copy(ids, i, ordinalsSeg, ValueLayout.JAVA_INT, 0L, count);
                bulkOffsetsImpl.invokeExact(dataset, query, dims, dims, ordinalsSeg, count, resultsSeg);
            }
        } catch (Throwable t) {
            throw rethrow(t);
        }
        // Copy last batch to return array for dead-code elimination prevention
        MemorySegment.copy(resultsSeg, ValueLayout.JAVA_FLOAT, 0L, scores, 0, scores.length);
        return scores;
    }

    @Benchmark
    public float[] scoreMultipleRandomBulk() {
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

    private static final VectorSimilarityFunctions vectorSimilarityFunctions = NativeAccess.instance()
        .getVectorSimilarityFunctions()
        .orElseThrow();
}
