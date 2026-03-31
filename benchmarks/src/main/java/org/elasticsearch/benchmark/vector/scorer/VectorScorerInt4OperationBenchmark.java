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
import static org.elasticsearch.nativeaccess.Int4TestUtils.dotProductI4SinglePacked;
import static org.elasticsearch.nativeaccess.Int4TestUtils.packNibbles;
import static org.elasticsearch.simdvec.internal.vectorization.VectorScorerTestUtils.randomInt4Bytes;

/**
 * Benchmark comparing raw int4 packed-nibble dot product implementations:
 * scalar (plain loop) vs Lucene (Panama-vectorized VectorUtil).
 * Run with ./gradlew -p benchmarks run --args 'VectorScorerInt4OperationBenchmark'
 */
@Fork(value = 3, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class VectorScorerInt4OperationBenchmark {

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

    @Param({ "2", "128", "208", "256", "300", "512", "702", "1024", "1536", "2048" })
    public int size;

    @Setup(Level.Iteration)
    public void init() {
        unpacked = new byte[size];
        randomInt4Bytes(ThreadLocalRandom.current(), unpacked);
        packed = packNibbles(unpacked);
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
            VectorSimilarityFunctions.DataType.INT4,
            VectorSimilarityFunctions.Operation.SINGLE
        );
    }

    @TearDown
    public void teardown() {
        arena.close();
    }

    @Benchmark
    public int scalar() {
        return dotProductI4SinglePacked(unpacked, packed);
    }

    @Benchmark
    public int lucene() {
        return VectorUtil.int4DotProductSinglePacked(unpacked, packed);
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

    static final VectorSimilarityFunctions vectorSimilarityFunctions = NativeAccess.instance().getVectorSimilarityFunctions().orElseThrow();
}
