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
import static org.elasticsearch.nativeaccess.Int2TestUtils.packQuads;

/**
 * Bare-bones bulk operation benchmark for int2 packed-quad vector dot product.
 * Dispatches directly to the native BULK / BULK_OFFSETS / BULK_SPARSE implementations
 * via {@link VectorSimilarityFunctions}, bypassing any Lucene scorer infrastructure:
 * <ul>
 *   <li>{@code scoreBulk} — contiguous slice (sequential by construction)</li>
 *   <li>{@code scoreBulkOffsets} — scattered access via offsets array</li>
 *   <li>{@code scoreBulkSparse} — scattered access via pre-resolved address array</li>
 * </ul>
 * {@code scoreSequential} and {@code scoreRandom} are single-pair controls.
 * <p>
 * Run with: {@code ./gradlew -p benchmarks run --args 'VectorScorerInt2BulkOperationBenchmark'}
 */
@Fork(value = 1, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
public class VectorScorerInt2BulkOperationBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    @Param({ "1024" })
    public int dims;

    // dims=1024 -> packed_len = 256 bytes per vector. The numVectors values below are
    // chosen to span cache regimes across the platforms we run on; for example:
    // c8a.xlarge (Zen 5, 4 cores): L1d=48KB/core, L2=1MB/core, L3=16MB shared
    // c8i.2xlarge (Xeon 6 6975P, 4 cores): L1d=48KB/core, L2=2MB/core, L3=480MB shared
    // c8g.xlarge (Graviton 4, 4 cores): L1d=64KB/core, L2=2MB/core, L3=36MB shared
    // c7g.xlarge (Graviton 3, 4 cores): L1d=64KB/core, L2=1MB/core, L3=32MB shared
    // Working-set sizes for the picks below:
    // 512 vectors = 128KB: overflows L1
    // 10000 vectors = 2.5MB: overflows L2
    // 2000000 vectors = ~488MB: overflows L3
    @Param({ "512", "10000", "2000000" })
    public int numVectors;

    @Param({ "32", "64", "256", "1024" })
    public int bulkSize;

    private Arena arena;

    // packed dataset: numVectors * (dims/4) bytes laid out contiguously in native memory
    private MemorySegment dataset;
    // unpacked query: dims bytes (4 stripes back-to-back, see Int2TestUtils for layout)
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
                randomInt2Bytes(random, unpacked);
                packedVectors[v] = packQuads(unpacked);
            }
            queryUnpacked = new byte[dims];
            randomInt2Bytes(random, queryUnpacked);
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
        assert dims % 4 == 0 : "Int2 requires dims divisible by 4 (4 values per packed byte): " + dims;
        arena = Arena.ofConfined();
        numVectorsToScore = vectorData.numVectorsToScore;
        packedLen = dims / 4;

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
            VectorSimilarityFunctions.DataType.INT2,
            VectorSimilarityFunctions.Operation.SINGLE
        );
        bulkImpl = vectorSimilarityFunctions.getHandle(
            VectorSimilarityFunctions.Function.DOT_PRODUCT,
            VectorSimilarityFunctions.DataType.INT2,
            VectorSimilarityFunctions.Operation.BULK
        );
        bulkOffsetsImpl = vectorSimilarityFunctions.getHandle(
            VectorSimilarityFunctions.Function.DOT_PRODUCT,
            VectorSimilarityFunctions.DataType.INT2,
            VectorSimilarityFunctions.Operation.BULK_OFFSETS
        );
        bulkSparseImpl = vectorSimilarityFunctions.getHandle(
            VectorSimilarityFunctions.Function.DOT_PRODUCT,
            VectorSimilarityFunctions.DataType.INT2,
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

    static void randomInt2Bytes(Random random, byte[] dst) {
        for (int i = 0; i < dst.length; i++) {
            dst[i] = (byte) random.nextInt(4);
        }
    }

    private static final VectorSimilarityFunctions vectorSimilarityFunctions = NativeAccess.instance()
        .getVectorSimilarityFunctions()
        .orElseThrow();
}
