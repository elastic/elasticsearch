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
import org.elasticsearch.index.codec.vectors.BFloat16;
import org.elasticsearch.nativeaccess.NativeAccess;
import org.elasticsearch.nativeaccess.VectorSimilarityFunctions;
import org.elasticsearch.nativeaccess.VectorSimilarityFunctions.BFloat16QueryType;
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
import java.nio.ByteOrder;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.rethrow;

/**
 * Bare-bones bulk operation benchmark for bfloat16 vector similarity functions.
 * Calls the native bulk_offsets operation directly via VectorSimilarityFunctions,
 * bypassing Lucene scorer infrastructure.
 * <p>
 * Run with: {@code ./gradlew -p benchmarks run --args 'VectorScorerBFloat16BulkOperationBenchmark'}
 */
@Fork(value = 1, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
public class VectorScorerBFloat16BulkOperationBenchmark {

    static final ValueLayout.OfShort LAYOUT_LE_SHORT = ValueLayout.JAVA_SHORT.withOrder(ByteOrder.LITTLE_ENDIAN);
    static final ValueLayout.OfFloat LAYOUT_LE_FLOAT = ValueLayout.JAVA_FLOAT.withOrder(ByteOrder.LITTLE_ENDIAN);

    static {
        Utils.configureBenchmarkLogging();
    }

    @Param({ "1024" })
    public int dims;

    // With dims=1024, each bf16 vector is 2KB. Target cache overflow points:
    // c8a (AMD EPYC): L1=48KB, L2=1MB, L3=16MB
    // c8i (Intel Xeon 6): L1=48KB, L2=2MB, L3=~100MB
    // 128 vectors = 256KB: overflows L1 on both, fits in L2
    // 2500 vectors = 5MB: overflows L2 on AMD, fits in L3
    // 65000 vectors = ~127MB: overflows L3 on both
    @Param({ "128", "2500", "65000" })
    public int numVectors;

    @Param({ "32", "64", "256", "1024" })
    public int bulkSize;

    @Param({ "DOT_PRODUCT", "EUCLIDEAN" })
    public VectorSimilarityType function;

    @Param
    public BFloat16QueryType queryType;

    private Arena arena;

    // Dataset: numVectors bf16 vectors laid out contiguously in native memory.
    private MemorySegment dataset;
    // Query vector in native memory (bf16 or f32 depending on queryType).
    private MemorySegment query;
    private int[] ids;
    private int[] ordinals;
    private int numVectorsToScore;
    // Scratch buffers in native memory for bulk calls.
    private MemorySegment ordinalsSeg;
    private MemorySegment resultsSeg;
    private float[] scores;

    private MethodHandle singleImpl;
    private MethodHandle bulkOffsetsImpl;

    record VectorData(int numVectorsToScore, short[][] bf16Vectors, float[] f32QueryVector, int[] ordinals, int targetOrd) {

        static VectorData create(int dims, int numVectors, int numVectorsToScore, Random random) {
            var bf16Vectors = new short[numVectors][];
            var f32QueryVector = new float[dims];

            var ordinals = BenchmarkUtils.generateRandomOrdinals(numVectors, numVectorsToScore, random);
            var targetOrd = random.nextInt(numVectors);

            for (int v = 0; v < numVectors; v++) {
                bf16Vectors[v] = new short[dims];
                for (int d = 0; d < dims; d++) {
                    float val = random.nextFloat();
                    bf16Vectors[v][d] = BFloat16.floatToBFloat16(val);
                    if (v == targetOrd) {
                        f32QueryVector[d] = val;
                    }
                }
            }

            return new VectorData(numVectorsToScore, bf16Vectors, f32QueryVector, ordinals, targetOrd);
        }
    }

    @Setup
    public void setup() {
        setup(VectorData.create(dims, numVectors, Math.min(numVectors, 20_000), ThreadLocalRandom.current()));
    }

    void setup(VectorData vectorData) {
        arena = Arena.ofConfined();

        numVectorsToScore = vectorData.numVectorsToScore;

        int bytesPerVector = dims * Short.BYTES;
        dataset = arena.allocate((long) numVectors * bytesPerVector);
        for (int v = 0; v < numVectors; v++) {
            MemorySegment.copy(vectorData.bf16Vectors[v], 0, dataset, LAYOUT_LE_SHORT, (long) v * bytesPerVector, dims);
        }

        // Query vector: use the target ordinal's vector in the appropriate type
        switch (queryType) {
            case BFLOAT16 -> {
                query = arena.allocate((long) dims * Short.BYTES);
                MemorySegment.copy(vectorData.bf16Vectors[vectorData.targetOrd], 0, query, LAYOUT_LE_SHORT, 0L, dims);
            }
            case FLOAT32 -> {
                query = arena.allocate((long) dims * Float.BYTES);
                MemorySegment.copy(vectorData.f32QueryVector, 0, query, LAYOUT_LE_FLOAT, 0L, dims);
            }
        }

        ids = IntStream.range(0, numVectors).toArray();
        ordinals = vectorData.ordinals;

        ordinalsSeg = arena.allocate((long) bulkSize * Integer.BYTES);
        resultsSeg = arena.allocate((long) bulkSize * Float.BYTES);
        scores = new float[bulkSize];

        VectorSimilarityFunctions.Function nativeFunc = switch (function) {
            case DOT_PRODUCT -> VectorSimilarityFunctions.Function.DOT_PRODUCT;
            case EUCLIDEAN -> VectorSimilarityFunctions.Function.SQUARE_DISTANCE;
            default -> throw new IllegalArgumentException(function.toString());
        };
        singleImpl = vectorSimilarityFunctions.getBFloat16Handle(nativeFunc, queryType, VectorSimilarityFunctions.Operation.SINGLE);
        bulkOffsetsImpl = vectorSimilarityFunctions.getBFloat16Handle(
            nativeFunc,
            queryType,
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
        int bytesPerVector = dims * Short.BYTES;
        try {
            while (v < numVectorsToScore) {
                for (int i = 0; i < bulkSize && v < numVectorsToScore; i++, v++) {
                    MemorySegment vec = dataset.asSlice((long) ids[v] * bytesPerVector, bytesPerVector);
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
        int bytesPerVector = dims * Short.BYTES;
        try {
            while (v < numVectorsToScore) {
                for (int i = 0; i < bulkSize && v < numVectorsToScore; i++, v++) {
                    MemorySegment vec = dataset.asSlice((long) ordinals[v] * bytesPerVector, bytesPerVector);
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
        int bytesPerVector = dims * Short.BYTES;
        try {
            for (int i = 0; i < numVectorsToScore; i += bulkSize) {
                int count = Math.min(bulkSize, numVectorsToScore - i);
                MemorySegment.copy(ids, i, ordinalsSeg, ValueLayout.JAVA_INT, 0L, count);
                bulkOffsetsImpl.invokeExact(dataset, query, dims, bytesPerVector, ordinalsSeg, count, resultsSeg);
            }
        } catch (Throwable t) {
            throw rethrow(t);
        }
        MemorySegment.copy(resultsSeg, ValueLayout.JAVA_FLOAT, 0L, scores, 0, scores.length);
        return scores;
    }

    @Benchmark
    public float[] scoreMultipleRandomBulk() {
        int bytesPerVector = dims * Short.BYTES;
        try {
            for (int i = 0; i < numVectorsToScore; i += bulkSize) {
                int count = Math.min(bulkSize, numVectorsToScore - i);
                MemorySegment.copy(ordinals, i, ordinalsSeg, ValueLayout.JAVA_INT, 0L, count);
                bulkOffsetsImpl.invokeExact(dataset, query, dims, bytesPerVector, ordinalsSeg, count, resultsSeg);
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
