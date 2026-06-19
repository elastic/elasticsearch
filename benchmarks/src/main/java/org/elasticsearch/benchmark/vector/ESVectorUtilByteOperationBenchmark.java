/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.vector;

import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.simdvec.ESVectorUtil;
import org.elasticsearch.simdvec.ESVectorizationProvider;
import org.elasticsearch.simdvec.internal.vectorization.ESVectorUtilSupport;
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

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks for {@link ESVectorUtil} byte range operations ({@code dotProduct} with length and
 * {@code l2Normalize}) comparing the default scalar implementation against the Panama SIMD path.
 */
@Fork(value = 1, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
@Warmup(iterations = 4, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
public class ESVectorUtilByteOperationBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    @Param({ "SCALAR", "PANAMA" })
    public VectorImplementation implementation;

    @Param({ "1", "128", "207", "256", "300", "512", "702", "1024", "1536", "2048" })
    public int size;

    byte[] a;
    byte[] b;
    byte[] normalizeSource;
    byte[] normalizeTarget;
    ESVectorUtilSupport impl;

    @Setup(Level.Trial)
    public void setup() {
        setup(ThreadLocalRandom.current());
    }

    public void setup(Random random) {
        a = new byte[size];
        b = new byte[size];
        normalizeSource = new byte[size];
        normalizeTarget = new byte[size];
        random.nextBytes(a);
        random.nextBytes(b);
        random.nextBytes(normalizeSource);
        for (int i = 0; i < size; i++) {
            if (normalizeSource[i] == 0) {
                normalizeSource[i] = 1;
            }
        }
        impl = switch (implementation) {
            case SCALAR -> ESVectorizationProvider.lookup(false, false).getVectorUtilSupport();
            case PANAMA -> ESVectorizationProvider.lookup(true, false).getVectorUtilSupport();
            default -> throw new IllegalArgumentException(implementation.toString());
        };
    }

    @Benchmark
    public float dotProduct() {
        return impl.dotProduct(a, b, 0, size);
    }

    @Benchmark
    public float l2Normalize() {
        System.arraycopy(normalizeSource, 0, normalizeTarget, 0, size);
        impl.l2Normalize(normalizeTarget, 0, size);
        return normalizeTarget[0];
    }
}
