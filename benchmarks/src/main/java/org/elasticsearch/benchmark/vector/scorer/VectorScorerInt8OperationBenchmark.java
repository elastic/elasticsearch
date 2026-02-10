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
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.logging.NodeNamePatternConverter;
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
import java.lang.invoke.MethodHandle;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.rethrow;

@Fork(value = 3, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class VectorScorerInt8OperationBenchmark {

    static {
        NodeNamePatternConverter.setGlobalNodeName("foo");
        LogConfigurator.loadLog4jPlugins();
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
    }

    byte[] bytesA;
    byte[] bytesB;
    byte[] scratch;
    MemorySegment heapSegA, heapSegB;
    MemorySegment nativeSegA, nativeSegB;

    Arena arena;

    @Param({ "1", "128", "207", "256", "300", "512", "702", "1024", "1536", "2048" })
    public int size;

    @Param({ "COSINE", "DOT_PRODUCT", "EUCLIDEAN" })
    public VectorSimilarityType function;

    @FunctionalInterface
    private interface LuceneFunction {
        float run(byte[] vec1, byte[] vec2);
    }

    private LuceneFunction luceneImpl;
    private MethodHandle nativeImpl;

    @Setup(Level.Iteration)
    public void init() {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        bytesA = new byte[size];
        bytesB = new byte[size];
        scratch = new byte[size];
        random.nextBytes(bytesA);
        random.nextBytes(bytesB);
        heapSegA = MemorySegment.ofArray(bytesA);
        heapSegB = MemorySegment.ofArray(bytesB);

        arena = Arena.ofConfined();
        nativeSegA = arena.allocate(bytesA.length);
        MemorySegment.copy(MemorySegment.ofArray(bytesA), JAVA_BYTE, 0L, nativeSegA, JAVA_BYTE, 0L, bytesA.length);
        nativeSegB = arena.allocate(bytesB.length);
        MemorySegment.copy(MemorySegment.ofArray(bytesB), JAVA_BYTE, 0L, nativeSegB, JAVA_BYTE, 0L, bytesB.length);

        luceneImpl = switch (function) {
            case COSINE -> VectorUtil::cosine;
            case DOT_PRODUCT -> VectorUtil::dotProduct;
            case EUCLIDEAN -> VectorUtil::squareDistance;
            default -> throw new UnsupportedOperationException("Not used");
        };
        nativeImpl = vectorSimilarityFunctions.getHandle(switch (function) {
            case COSINE -> VectorSimilarityFunctions.Function.COSINE;
            case DOT_PRODUCT -> VectorSimilarityFunctions.Function.DOT_PRODUCT;
            case EUCLIDEAN -> VectorSimilarityFunctions.Function.SQUARE_DISTANCE;
            default -> throw new IllegalArgumentException(function.toString());
        }, VectorSimilarityFunctions.DataType.INT8, VectorSimilarityFunctions.Operation.SINGLE);
    }

    @TearDown
    public void teardown() {
        arena.close();
    }

    @Benchmark
    public float lucene() {
        return luceneImpl.run(bytesA, bytesB);
    }

    @Benchmark
    public float luceneWithCopy() {
        // add a copy to better reflect what Lucene has to do to get the target vector on-heap
        MemorySegment.copy(nativeSegB, JAVA_BYTE, 0L, scratch, 0, scratch.length);
        return luceneImpl.run(bytesA, scratch);
    }

    @Benchmark
    public float nativeWithNativeSeg() {
        try {
            return (float) nativeImpl.invokeExact(nativeSegA, nativeSegB, size);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    @Benchmark
    public float nativeWithHeapSeg() {
        try {
            return (float) nativeImpl.invokeExact(heapSegA, heapSegB, size);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    static final VectorSimilarityFunctions vectorSimilarityFunctions = NativeAccess.instance().getVectorSimilarityFunctions().orElseThrow();
}
