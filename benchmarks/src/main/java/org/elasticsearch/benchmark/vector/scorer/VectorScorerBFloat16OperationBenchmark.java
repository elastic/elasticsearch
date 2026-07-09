/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.benchmark.vector.scorer;

import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.index.codec.vectors.BFloat16;
import org.elasticsearch.nativeaccess.NativeAccess;
import org.elasticsearch.nativeaccess.VectorSimilarityFunctions;
import org.elasticsearch.simdvec.VectorSimilarityType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ShortBuffer;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark comparing BFloat16 implementations
 * Run with ./gradlew -p benchmarks run --args 'VectorScorerBFloat16OperationBenchmark'
 */
@Fork(value = 3, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class VectorScorerBFloat16OperationBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    static final ValueLayout.OfByte LAYOUT_LE_BFLOAT16 = ValueLayout.JAVA_BYTE;
    static final ValueLayout.OfFloat LAYOUT_LE_FLOAT32 = ValueLayout.JAVA_FLOAT.withOrder(ByteOrder.LITTLE_ENDIAN);

    byte[] bFloatsA;
    byte[] bFloatsB;
    float[] floatsB;
    float[] scratchA;
    float[] scratchB;
    MemorySegment heapSegA, heapSegB;
    MemorySegment nativeSegA, nativeSegB;

    Arena arena;

    @Param({ "1", "128", "207", "256", "300", "512", "702", "1024", "1536", "2048" })
    public int size;

    @Param({ "DOT_PRODUCT", "EUCLIDEAN" })
    public VectorSimilarityType function;

    @Param
    public VectorSimilarityFunctions.BFloat16QueryType queryType;

    private LuceneFunction<float[]> luceneImpl;

    @Setup(Level.Iteration)
    public void init() {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        bFloatsA = new byte[size * 2];
        bFloatsB = new byte[size * 2];
        floatsB = new float[size];
        scratchA = new float[size];
        scratchB = new float[size];
        ShortBuffer bfloatsAShorts = ByteBuffer.wrap(bFloatsA).order(ByteOrder.LITTLE_ENDIAN).asShortBuffer();
        ShortBuffer bfloatsBShorts = ByteBuffer.wrap(bFloatsB).order(ByteOrder.LITTLE_ENDIAN).asShortBuffer();
        for (int i = 0; i < size; ++i) {
            bfloatsAShorts.put(BFloat16.floatToBFloat16(random.nextFloat()));
            floatsB[i] = random.nextFloat();
            bfloatsBShorts.put(BFloat16.floatToBFloat16(floatsB[i]));
        }
        heapSegA = MemorySegment.ofArray(bFloatsA);
        heapSegB = switch (queryType) {
            case BFLOAT16 -> MemorySegment.ofArray(bFloatsB);
            case FLOAT32 -> MemorySegment.ofArray(floatsB);
        };

        arena = Arena.ofConfined();
        nativeSegA = arena.allocate(bFloatsA.length);
        MemorySegment.copy(bFloatsA, 0, nativeSegA, LAYOUT_LE_BFLOAT16, 0L, bFloatsA.length);
        switch (queryType) {
            case BFLOAT16 -> {
                nativeSegB = arena.allocate(bFloatsB.length);
                MemorySegment.copy(bFloatsB, 0, nativeSegB, LAYOUT_LE_BFLOAT16, 0L, bFloatsB.length);
            }
            case FLOAT32 -> {
                nativeSegB = arena.allocate((long) floatsB.length * Float.BYTES);
                MemorySegment.copy(floatsB, 0, nativeSegB, LAYOUT_LE_FLOAT32, 0L, floatsB.length);
            }
        }

        luceneImpl = switch (function) {
            case DOT_PRODUCT -> VectorUtil::dotProduct;
            case EUCLIDEAN -> VectorUtil::squareDistance;
            default -> throw new UnsupportedOperationException("Not used");
        };
    }

    @TearDown
    public void teardown() {
        arena.close();
    }

    @Benchmark
    public float lucene() {
        // copy and convert the bfloats to floats as part of the test
        BFloat16.bFloat16ToFloat(bFloatsA, scratchA);
        return switch (queryType) {
            case BFLOAT16 -> {
                BFloat16.bFloat16ToFloat(bFloatsB, scratchB);
                yield luceneImpl.run(scratchA, scratchB);
            }
            case FLOAT32 -> luceneImpl.run(scratchA, floatsB);
        };
    }

    @Benchmark
    public float nativeWithNativeSeg() {
        return switch (function) {
            case DOT_PRODUCT -> switch (queryType) {
                case FLOAT32 -> vectorSimilarityFunctions.dotProductDBF16QF32(nativeSegA, nativeSegB, size);
                case BFLOAT16 -> vectorSimilarityFunctions.dotProductDBF16QBF16(nativeSegA, nativeSegB, size);
            };
            case EUCLIDEAN -> switch (queryType) {
                case FLOAT32 -> vectorSimilarityFunctions.squareDistanceDBF16QF32(nativeSegA, nativeSegB, size);
                case BFLOAT16 -> vectorSimilarityFunctions.squareDistanceDBF16QBF16(nativeSegA, nativeSegB, size);
            };
            default -> throw new IllegalArgumentException(function.toString());
        };
    }

    @Benchmark
    public float nativeWithHeapSeg() {
        return switch (function) {
            case DOT_PRODUCT -> switch (queryType) {
                case FLOAT32 -> vectorSimilarityFunctions.dotProductDBF16QF32(heapSegA, heapSegB, size);
                case BFLOAT16 -> vectorSimilarityFunctions.dotProductDBF16QBF16(heapSegA, heapSegB, size);
            };
            case EUCLIDEAN -> switch (queryType) {
                case FLOAT32 -> vectorSimilarityFunctions.squareDistanceDBF16QF32(heapSegA, heapSegB, size);
                case BFLOAT16 -> vectorSimilarityFunctions.squareDistanceDBF16QBF16(heapSegA, heapSegB, size);
            };
            default -> throw new IllegalArgumentException(function.toString());
        };
    }

    static final VectorSimilarityFunctions vectorSimilarityFunctions = NativeAccess.instance().getVectorSimilarityFunctions().orElseThrow();
}
