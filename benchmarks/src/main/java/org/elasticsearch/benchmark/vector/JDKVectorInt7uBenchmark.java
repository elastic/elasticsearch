/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.benchmark.vector;

import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.logging.NodeNamePatternConverter;
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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class JDKVectorInt7uBenchmark {

    static {
        NodeNamePatternConverter.setGlobalNodeName("foo");
        LogConfigurator.loadLog4jPlugins();
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
    }

    byte[] byteArrayA;
    byte[] byteArrayB;
    MemorySegment heapSegA, heapSegB;
    MemorySegment nativeSegA, nativeSegB;

    Arena arena;

    @Param({ "1", "128", "207", "256", "300", "512", "702", "1024", "1536", "2048" })
    public int size;

    @Setup(Level.Iteration)
    public void init() {
        byteArrayA = new byte[size];
        byteArrayB = new byte[size];
        for (int i = 0; i < size; ++i) {
            randomInt7BytesBetween(byteArrayA);
            randomInt7BytesBetween(byteArrayB);
        }
        heapSegA = MemorySegment.ofArray(byteArrayA);
        heapSegB = MemorySegment.ofArray(byteArrayB);

        arena = Arena.ofConfined();
        nativeSegA = arena.allocate((long) byteArrayA.length);
        MemorySegment.copy(MemorySegment.ofArray(byteArrayA), 0L, nativeSegA, 0L, byteArrayA.length);
        nativeSegB = arena.allocate((long) byteArrayB.length);
        MemorySegment.copy(MemorySegment.ofArray(byteArrayB), 0L, nativeSegB, 0L, byteArrayB.length);
    }

    @TearDown
    public void teardown() {
        arena.close();
    }

    @Benchmark
    @Fork(value = 3, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
    public int dotProductLucene() {
        return VectorUtil.dotProduct(byteArrayA, byteArrayB);
    }

    @Benchmark
    @Fork(value = 3, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
    public int dotProductNativeWithNativeSeg() {
        return dotProduct7u(nativeSegA, nativeSegB, size);
    }

    @Benchmark
    @Fork(value = 3, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
    public int dotProductNativeWithHeapSeg() {
        return dotProduct7u(heapSegA, heapSegB, size);
    }

    static final VectorSimilarityFunctions vectorSimilarityFunctions = vectorSimilarityFunctions();

    static VectorSimilarityFunctions vectorSimilarityFunctions() {
        return NativeAccess.instance().getVectorSimilarityFunctions().get();
    }

    int dotProduct7u(MemorySegment a, MemorySegment b, int length) {
        try {
            return (int) vectorSimilarityFunctions.dotProductHandle7u().invokeExact(a, b, length);
        } catch (Throwable e) {
            if (e instanceof Error err) {
                throw err;
            } else if (e instanceof RuntimeException re) {
                throw re;
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    // Unsigned int7 byte vectors have values in the range of 0 to 127 (inclusive).
    static final byte MIN_INT7_VALUE = 0;
    static final byte MAX_INT7_VALUE = 127;

    static void randomInt7BytesBetween(byte[] bytes) {
        var random = ThreadLocalRandom.current();
        for (int i = 0, len = bytes.length; i < len;) {
            bytes[i++] = (byte) random.nextInt(MIN_INT7_VALUE, MAX_INT7_VALUE + 1);
        }
    }
}
