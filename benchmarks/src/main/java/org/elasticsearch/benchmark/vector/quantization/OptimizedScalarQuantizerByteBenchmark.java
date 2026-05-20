/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.vector.quantization;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer;
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
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks for {@link OptimizedScalarQuantizer#scalarQuantize(byte[], float[], int[], byte, byte[])}
 * — the byte[],byte[] quantization path used during indexing of byte-element BBQ vectors.
 *
 * <p>This exercises the Panama SIMD multi-part widening path in
 * {@code PanamaESVectorUtilSupport.centerAndCalculateOSQStats{Euclidean,Dp}(byte[], byte[], float[], float[])}.
 *
 * <pre>
 *   ./gradlew :benchmarks:jmh -Pargs='OptimizedScalarQuantizerByteBenchmark'
 * </pre>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 3)
public class OptimizedScalarQuantizerByteBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    @Param({ "384", "768", "1024" })
    int dims;

    @Param({ "1", "4", "7" })
    byte bits;

    @Param({ "EUCLIDEAN", "DOT_PRODUCT" })
    VectorSimilarityFunction similarityFunction;

    byte[] vector;
    byte[] centroid;
    float[] scratch;
    int[] destination;

    OptimizedScalarQuantizer osq;

    @Setup(Level.Iteration)
    public void init() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        osq = new OptimizedScalarQuantizer(similarityFunction);
        destination = new int[dims];
        vector = new byte[dims];
        centroid = new byte[dims];
        scratch = new float[dims];
        random.nextBytes(vector);
        random.nextBytes(centroid);
    }

    @Benchmark
    public int[] scalar() {
        osq.scalarQuantize(vector, scratch, destination, bits, centroid);
        return destination;
    }

    @Benchmark
    @Fork(jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
    public int[] vector() {
        osq.scalarQuantize(vector, scratch, destination, bits, centroid);
        return destination;
    }
}
