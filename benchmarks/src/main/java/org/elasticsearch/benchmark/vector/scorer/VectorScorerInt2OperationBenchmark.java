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
import java.lang.invoke.MethodHandle;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.rethrow;
import static org.elasticsearch.nativeaccess.Int2TestUtils.dotProductI2SinglePacked;
import static org.elasticsearch.nativeaccess.Int2TestUtils.packQuads;

/**
 * Benchmark comparing raw int2 packed-quad dot product implementations:
 * scalar (plain loop) vs native.
 * Run with ./gradlew -p benchmarks run --args 'VectorScorerInt2OperationBenchmark'
 */
@Fork(value = 3, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class VectorScorerInt2OperationBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    private int packedLen;

    private byte[] unpacked;
    private byte[] packed;

    MemorySegment unpackedHeapSeg, packedHeapSeg;
    MemorySegment unpackedNativeSeg, packedNativeSeg;

    Arena arena;

    private MethodHandle nativeImpl;

    @Param({ "4", "128", "256", "512", "1024", "1536", "2048" })
    public int size;

    @Setup(Level.Iteration)
    public void init() {
        assert size % 4 == 0 : "Int2 requires size divisible by 4 (4 values per packed byte): " + size;
        unpacked = new byte[size];
        randomInt2Bytes(ThreadLocalRandom.current(), unpacked);
        packed = packQuads(unpacked);
        packedLen = packed.length;

        unpackedHeapSeg = MemorySegment.ofArray(unpacked);
        packedHeapSeg = MemorySegment.ofArray(packed);

        arena = Arena.ofConfined();
        unpackedNativeSeg = arena.allocate(unpacked.length);
        MemorySegment.copy(unpacked, 0, unpackedNativeSeg, ValueLayout.JAVA_BYTE, 0L, unpacked.length);
        packedNativeSeg = arena.allocate(packed.length);
        MemorySegment.copy(packed, 0, packedNativeSeg, ValueLayout.JAVA_BYTE, 0L, packed.length);

        nativeImpl = vectorSimilarityFunctions.getHandle(
            VectorSimilarityFunctions.Function.DOT_PRODUCT,
            VectorSimilarityFunctions.DataType.INT2,
            VectorSimilarityFunctions.Operation.SINGLE
        );
    }

    @TearDown
    public void teardown() {
        arena.close();
    }

    @Benchmark
    public int scalar() {
        return dotProductI2SinglePacked(unpacked, packed);
    }

    @Benchmark
    public int nativeWithNativeSeg() {
        try {
            return (int) nativeImpl.invokeExact(unpackedNativeSeg, packedNativeSeg, packedLen);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    @Benchmark
    public int nativeWithHeapSeg() {
        try {
            return (int) nativeImpl.invokeExact(unpackedHeapSeg, packedHeapSeg, packedLen);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    static void randomInt2Bytes(java.util.Random random, byte[] dst) {
        for (int i = 0; i < dst.length; i++) {
            dst[i] = (byte) random.nextInt(4);
        }
    }

    static final VectorSimilarityFunctions vectorSimilarityFunctions = NativeAccess.instance().getVectorSimilarityFunctions().orElseThrow();
}
