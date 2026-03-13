/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.vector.quantization;

import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.index.codec.vectors.diskbbq.Preconditioner;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 30, time = 1)
@Fork(value = 1)
public class PreconditionerBenchmark {

    static {
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
    }

    @Param({ "500000" })
    int numVectors;

    @Param({ "384", "782", "1024" })
    int dims;

    @Param({ "32", "64" })
    int blockDims;

    float[][] vectors;
    Preconditioner preconditioner;

    @Setup
    public void setup() throws IOException {
        Random random = new Random(42L);
        vectors = new float[numVectors][dims];
        for (float[] vector : vectors) {
            for (int i = 0; i < dims; i++) {
                vector[i] = random.nextFloat();
            }
        }
        preconditioner = Preconditioner.createPreconditioner(dims, blockDims);
    }

    @Benchmark
    @Fork(jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
    public void applyTransformPanama(Blackhole bh) {
        float[] out = new float[dims];
        for (int i = 0; i < numVectors; i++) {
            preconditioner.applyTransform(vectors[i], out);
        }
    }

    @Benchmark
    public void applyTransformDefault(Blackhole bh) {
        float[] out = new float[dims];
        for (int i = 0; i < numVectors; i++) {
            preconditioner.applyTransform(vectors[i], out);
        }
    }
}
