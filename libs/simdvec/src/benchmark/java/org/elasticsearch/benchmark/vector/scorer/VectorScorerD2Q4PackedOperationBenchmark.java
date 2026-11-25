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
import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.benchmark.vector.VectorizationInfo;
import org.elasticsearch.simdvec.BBQTestUtils;
import org.elasticsearch.simdvec.BulkMask;
import org.elasticsearch.simdvec.SimdVecLibrary;
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

/**
 * Kernel-direct benchmark for the 2-bit-doc / 4-bit-query <b>packed-quad</b> BBQ dot product
 * ({@code vec_dotd2q4_packed}). Doc layout is four 2-bit values per byte; query layout is one
 * byte per value (range 0..15). Three modes are exercised on the same dataset:
 * <ul>
 *   <li>{@code scoreSingle} — single-pair calls in a sequential walk (control)</li>
 *   <li>{@code scoreBulk} — contiguous bulk slice</li>
 *   <li>{@code scoreBulkMasked} — the {@code selected} vectors of each block of 32, via a bit mask</li>
 * </ul>
 * <p>
 * Run with: {@code ./gradlew -p benchmarks run --args 'VectorScorerD2Q4PackedOperationBenchmark'}
 */
@Fork(value = 1, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
public class VectorScorerD2Q4PackedOperationBenchmark {

    static {
        BenchmarkLogging.configure();
        VectorizationInfo.printOnce();
    }

    @Param({ "128", "256", "512", "1024", "1536", "2048" })
    public int dims;

    // Doc bytes per vector are dims/4 so cache-regime breakpoints match:
    // 512 vectors @ dims=1024 = 128KB (overflows L1),
    // 10000 = 2.5MB (overflows L2),
    // 2000000 = ~488MB (overflows L3 on most cores).
    @Param({ "512", "10000", "2000000" })
    public int numVectors;

    @Param({ "32" })
    public int bulkSize;

    // vectors selected in each block of 32 by scoreBulkMasked
    @Param({ "32", "29", "3" })
    public int selected;

    private Arena arena;

    // dataset: numVectors * docBytes laid out contiguously in native memory
    private MemorySegment dataset;
    // query: dims bytes, one byte per source value (flat — packAsBytes-style)
    private MemorySegment query;
    // one mask per block of 32 consecutive vectors
    private int[] masks;
    private int numVectorsToScore;
    // doc bytes per vector = dims/4 (4 source values per packed-quad byte). This is also the
    // length parameter passed to the kernel.
    private int docBytes;
    // native scratch for bulk results
    private MemorySegment resultsSeg;
    // Java-side results returned from each @Benchmark to suppress dead-code elimination
    private float[] scores;

    static final class VectorData extends VectorScorerBulkBenchmark.VectorData {
        private final byte[][] packedDocs;
        private final byte[] flatQuery;
        private final long maskSeed;

        VectorData(int dims, int numVectors, int numVectorsToScore, Random random) {
            super(numVectors, numVectorsToScore, random, DataAccessPattern.RANDOM);
            packedDocs = new byte[numVectors][];
            byte[] unpackedDoc = new byte[dims];
            for (int v = 0; v < numVectors; v++) {
                randomBytes(random, unpackedDoc, 4);
                packedDocs[v] = BBQTestUtils.packQuads(unpackedDoc);
            }
            flatQuery = new byte[dims];
            randomBytes(random, flatQuery, 16);
            maskSeed = random.nextLong();
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
        assert dims % 4 == 0 : "D2Q4 packed requires dims divisible by 4: " + dims;
        arena = Arena.ofConfined();
        numVectorsToScore = vectorData.numVectorsToScore;
        docBytes = dims / 4;

        dataset = arena.allocate((long) numVectors * docBytes);
        for (int v = 0; v < numVectors; v++) {
            MemorySegment.copy(vectorData.packedDocs[v], 0, dataset, ValueLayout.JAVA_BYTE, (long) v * docBytes, docBytes);
        }

        query = arena.allocate(dims);
        MemorySegment.copy(vectorData.flatQuery, 0, query, ValueLayout.JAVA_BYTE, 0L, dims);

        masks = BenchmarkUtils.generateBlockMasks(numVectorsToScore, selected, new Random(vectorData.maskSeed));

        resultsSeg = arena.allocate((long) bulkSize * Float.BYTES);
        scores = new float[bulkSize];
    }

    @TearDown
    public void teardown() {
        arena.close();
    }

    /** Single-pair scoring, sequential walk (control vs scoreBulk). */
    @Benchmark
    public float[] scoreSingle() {
        int v = 0;
        while (v < numVectorsToScore) {
            for (int i = 0; i < bulkSize && v < numVectorsToScore; i++, v++) {
                MemorySegment vec = dataset.asSlice((long) v * docBytes, docBytes);
                scores[i] = VEC_LIBRARY.dotProductD2Q4Packed(vec, query, docBytes);
            }
        }
        return scores;
    }

    /** BULK: contiguous slice, sequential by construction. */
    @Benchmark
    public float[] scoreBulk() {
        for (int i = 0; i < numVectorsToScore; i += bulkSize) {
            int count = Math.min(bulkSize, numVectorsToScore - i);
            MemorySegment slice = dataset.asSlice((long) i * docBytes, (long) count * docBytes);
            VEC_LIBRARY.dotProductD2Q4PackedBulk(slice, query, docBytes, count, resultsSeg);
        }
        MemorySegment.copy(resultsSeg, ValueLayout.JAVA_FLOAT, 0L, scores, 0, scores.length);
        return scores;
    }

    /** BULK_MASKED: in each block of 32 consecutive vectors, only those selected by the block's mask. */
    @Benchmark
    public float[] scoreBulkMasked() {
        for (int b = 0; b < masks.length; b++) {
            int start = b * Integer.SIZE;
            int count = Math.min(Integer.SIZE, numVectorsToScore - start);
            MemorySegment block = dataset.asSlice((long) start * docBytes, (long) count * docBytes);
            VEC_LIBRARY.dotProductD2Q4PackedBulkMasked(block, query, docBytes, docBytes, masks[b], count, resultsSeg);
        }
        MemorySegment.copy(resultsSeg, ValueLayout.JAVA_FLOAT, 0L, scores, 0, scores.length);
        return scores;
    }

    /** Test-only: what {@link #scoreBulkMasked} returns, by single-pair scoring. */
    float[] scoreSingleMasked() {
        for (int b = 0; b < masks.length; b++) {
            for (int m = masks[b]; m != 0; m = BulkMask.deselectFirst(m)) {
                int slot = BulkMask.first(m);
                MemorySegment vec = dataset.asSlice((long) (b * Integer.SIZE + slot) * docBytes, docBytes);
                scores[slot] = VEC_LIBRARY.dotProductD2Q4Packed(vec, query, docBytes);
            }
        }
        return scores;
    }

    private static void randomBytes(Random random, byte[] dst, int rangeExcl) {
        for (int i = 0; i < dst.length; i++) {
            dst[i] = (byte) random.nextInt(rangeExcl);
        }
    }

    private static final SimdVecLibrary VEC_LIBRARY = SimdVecLibrary.instance().orElseThrow();
}
