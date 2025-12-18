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
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.rethrow;

@Fork(value = 3, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class VectorScorerFloat32OperationBenchmark {

    static {
        NodeNamePatternConverter.setGlobalNodeName("foo");
        LogConfigurator.loadLog4jPlugins();
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
    }

    static final ValueLayout.OfFloat LAYOUT_LE_FLOAT = ValueLayout.JAVA_FLOAT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

    float[] floatsA;
    float[] floatsB;
    float[] scratch;
    MemorySegment heapSegA, heapSegB;
    MemorySegment nativeSegA, nativeSegB;

    Arena arena;

    @Param({ "1", "128", "207", "256", "300", "512", "702", "1024", "1536", "2048" })
    public int size;

    @Param({ "COSINE", "DOT_PRODUCT", "EUCLIDEAN" })
    public VectorSimilarityType function;

    @FunctionalInterface
    private interface LuceneFunction {
        float run(float[] vec1, float[] vec2);
    }

    @FunctionalInterface
    private interface NativeFunction {
        float run(MemorySegment vec1, MemorySegment vec2, int length);
    }

    private LuceneFunction luceneImpl;
    private NativeFunction nativeImpl;

    @Setup(Level.Iteration)
    public void init() {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        floatsA = new float[size];
        floatsB = new float[size];
        scratch = new float[size];
        for (int i = 0; i < size; ++i) {
            floatsA[i] = random.nextFloat();
            floatsB[i] = random.nextFloat();
        }
        heapSegA = MemorySegment.ofArray(floatsA);
        heapSegB = MemorySegment.ofArray(floatsB);

        arena = Arena.ofConfined();
        nativeSegA = arena.allocate((long) floatsA.length * Float.BYTES);
        MemorySegment.copy(MemorySegment.ofArray(floatsA), LAYOUT_LE_FLOAT, 0L, nativeSegA, LAYOUT_LE_FLOAT, 0L, floatsA.length);
        nativeSegB = arena.allocate((long) floatsB.length * Float.BYTES);
        MemorySegment.copy(MemorySegment.ofArray(floatsB), LAYOUT_LE_FLOAT, 0L, nativeSegB, LAYOUT_LE_FLOAT, 0L, floatsB.length);

        luceneImpl = switch (function) {
            case COSINE -> VectorUtil::cosine;
            case DOT_PRODUCT -> VectorUtil::dotProduct;
            case EUCLIDEAN -> VectorUtil::squareDistance;
            default -> throw new UnsupportedOperationException("Not used");
        };
        nativeImpl = switch (function) {
            case COSINE -> VectorScorerFloat32OperationBenchmark::cosineFloat32;
            case DOT_PRODUCT -> VectorScorerFloat32OperationBenchmark::dotProductFloat32;
            case EUCLIDEAN -> VectorScorerFloat32OperationBenchmark::squareDistanceFloat32;
            default -> throw new UnsupportedOperationException("Not used");
        };
    }

    @TearDown
    public void teardown() {
        arena.close();
    }

    @Benchmark
    public float lucene() {
        return luceneImpl.run(floatsA, floatsB);
    }

    @Benchmark
    public float luceneWithCopy() {
        // add a copy to better reflect what Lucene has to do to get the target vector on-heap
        MemorySegment.copy(nativeSegB, LAYOUT_LE_FLOAT, 0L, scratch, 0, scratch.length);
        return luceneImpl.run(floatsA, scratch);
    }

    @Benchmark
    public float nativeWithNativeSeg() {
        return nativeImpl.run(nativeSegA, nativeSegB, size);
    }

    @Benchmark
    public float nativeWithHeapSeg() {
        return nativeImpl.run(heapSegA, heapSegB, size);
    }

    static final VectorSimilarityFunctions vectorSimilarityFunctions = vectorSimilarityFunctions();

    static VectorSimilarityFunctions vectorSimilarityFunctions() {
        return NativeAccess.instance().getVectorSimilarityFunctions().get();
    }

    static float cosineFloat32(MemorySegment a, MemorySegment b, int length) {
        try {
            return (float) vectorSimilarityFunctions.cosineHandleFloat32().invokeExact(a, b, length);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    static float dotProductFloat32(MemorySegment a, MemorySegment b, int length) {
        try {
            return (float) vectorSimilarityFunctions.dotProductHandleFloat32().invokeExact(a, b, length);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    static float squareDistanceFloat32(MemorySegment a, MemorySegment b, int length) {
        try {
            return (float) vectorSimilarityFunctions.squareDistanceHandleFloat32().invokeExact(a, b, length);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }
}
