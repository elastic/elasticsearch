/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.vector;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.elasticsearch.common.logging.LogConfigurator;
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

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 3)
public class OptimizedScalarQuantizerBenchmark {
    static {
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
    }
    @Param({ "384", "702", "1024" })
    int dims;

    float[] vector;
    float[] centroid;
    byte[] destination;

    @Param({ "1", "4", "7" })
    byte bits;

    OptimizedScalarQuantizer osq = new OptimizedScalarQuantizer(VectorSimilarityFunction.DOT_PRODUCT);

    @Setup(Level.Iteration)
    public void init() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        // random byte arrays for binary methods
        destination = new byte[dims];
        vector = new float[dims];
        centroid = new float[dims];
        for (int i = 0; i < dims; ++i) {
            vector[i] = random.nextFloat();
            centroid[i] = random.nextFloat();
        }
    }

    @Benchmark
    public byte[] scalar() {
        osq.scalarQuantize(vector, destination, bits, centroid);
        return destination;
    }

    @Benchmark
    @Fork(jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
    public byte[] vector() {
        osq.scalarQuantize(vector, destination, bits, centroid);
        return destination;
    }
}
