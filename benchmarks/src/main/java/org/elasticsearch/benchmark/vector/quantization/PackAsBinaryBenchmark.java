/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.benchmark.vector.quantization;

import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.benchmark.vector.VectorImplementation;
import org.elasticsearch.index.codec.vectors.BQVectorUtils;
import org.elasticsearch.simdvec.ESVectorizationProvider;
import org.elasticsearch.simdvec.internal.vectorization.ESVectorUtilSupport;
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

@Fork(value = 1, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
// first iteration is complete garbage, so make sure we really warmup
@Warmup(iterations = 4, time = 1)
// real iterations. not useful to spend tons of time here, better to fork more
@Measurement(iterations = 5, time = 1)
public class PackAsBinaryBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    @Param({ "384", "782", "1024" })
    int dims;

    @Param({ "SCALAR", "PANAMA" })
    VectorImplementation implementation;

    int length;

    int numVectors = 1000;

    int[][] qVectors;
    byte[] packed;
    ESVectorUtilSupport impl;

    @Setup
    public void setup() throws IOException {
        Random random = new Random(123);

        this.length = BQVectorUtils.discretize(dims, 64) / 8;
        this.packed = new byte[length];

        qVectors = new int[numVectors][dims];
        for (int[] qVector : qVectors) {
            for (int i = 0; i < dims; i++) {
                qVector[i] = random.nextInt(2);
            }
        }

        impl = switch (implementation) {
            case SCALAR -> ESVectorizationProvider.lookup(false, false).getVectorUtilSupport();
            case PANAMA -> ESVectorizationProvider.lookup(true, false).getVectorUtilSupport();
            default -> throw new IllegalArgumentException(implementation.toString());
        };
    }

    @Benchmark
    public void packAsBinary(Blackhole bh) {
        for (int i = 0; i < numVectors; i++) {
            impl.packAsBinary(qVectors[i], packed);
            bh.consume(packed);
        }
    }
}
