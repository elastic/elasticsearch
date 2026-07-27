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
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.randomInt7BytesBetween;

@Fork(value = 3, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class VectorScorerInt7uOperationBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    byte[] byteArrayA;
    byte[] byteArrayB;
    MemorySegment heapSegA, heapSegB;
    MemorySegment nativeSegA, nativeSegB;

    Arena arena;

    @Param({ "1", "128", "207", "256", "300", "512", "702", "1024", "1536", "2048" })
    public int size;

    @Param({ "DOT_PRODUCT", "EUCLIDEAN" })
    public VectorSimilarityType function;

    private LuceneFunction<byte[]> luceneImpl;

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
        nativeSegA = arena.allocate(byteArrayA.length);
        MemorySegment.copy(byteArrayA, 0, nativeSegA, ValueLayout.JAVA_BYTE, 0L, byteArrayA.length);
        nativeSegB = arena.allocate(byteArrayB.length);
        MemorySegment.copy(byteArrayB, 0, nativeSegB, ValueLayout.JAVA_BYTE, 0L, byteArrayB.length);

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
        return luceneImpl.run(byteArrayA, byteArrayB);
    }

    @Benchmark
    public int nativeWithNativeSeg() {
        return switch (function) {
            case DOT_PRODUCT -> vectorSimilarityFunctions.dotProductI7u(nativeSegA, nativeSegB, size);
            case EUCLIDEAN -> vectorSimilarityFunctions.squareDistanceI7u(nativeSegA, nativeSegB, size);
            default -> throw new IllegalArgumentException(function.toString());
        };
    }

    @Benchmark
    public int nativeWithHeapSeg() {
        return switch (function) {
            case DOT_PRODUCT -> vectorSimilarityFunctions.dotProductI7u(heapSegA, heapSegB, size);
            case EUCLIDEAN -> vectorSimilarityFunctions.squareDistanceI7u(heapSegA, heapSegB, size);
            default -> throw new IllegalArgumentException(function.toString());
        };
    }

    static final VectorSimilarityFunctions vectorSimilarityFunctions = NativeAccess.instance().getVectorSimilarityFunctions().orElseThrow();
}
