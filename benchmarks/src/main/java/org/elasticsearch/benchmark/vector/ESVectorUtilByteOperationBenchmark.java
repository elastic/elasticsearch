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
 * Benchmarks for {@link ESVectorUtil} byte range operations ({@code dotProduct} with length and
 * {@code l2Normalize}) comparing the default scalar implementation against the Panama SIMD path.
 */
@Warmup(iterations = 4, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Fork(value = 1)
public class ESVectorUtilByteOperationBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    public enum Operation {
        DOT_PRODUCT,
        L2_NORMALIZE
    }

    @Param({ "1", "128", "207", "256", "300", "512", "702", "1024", "1536", "2048" })
    public int size;

    @Param
    public Operation operation;

    byte[] a;
    byte[] b;
    byte[] normalizeSource;
    byte[] normalizeTarget;

    @Setup(Level.Trial)
    public void setup() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
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
    }

    @Benchmark
    public float scalar() {
        return switch (operation) {
            case DOT_PRODUCT -> scalarDotProduct(a, b, 0, size);
            case L2_NORMALIZE -> {
                System.arraycopy(normalizeSource, 0, normalizeTarget, 0, size);
                scalarL2Normalize(normalizeTarget, 0, size);
                yield normalizeTarget[0];
            }
        };
    }

    @Benchmark
    @Fork(jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
    public float panamaSimd() {
        return switch (operation) {
            case DOT_PRODUCT -> ESVectorUtil.dotProduct(a, b, size);
            case L2_NORMALIZE -> {
                System.arraycopy(normalizeSource, 0, normalizeTarget, 0, size);
                ESVectorUtil.l2Normalize(normalizeTarget, size);
                yield normalizeTarget[0];
            }
        };
    }

    static float scalarDotProduct(byte[] a, byte[] b, int offset, int length) {
        int sum = 0;
        int end = offset + length;
        for (int i = offset; i < end; i++) {
            sum += a[i] * b[i];
        }
        return sum;
    }

    static void scalarL2Normalize(byte[] v, int offset, int length) {
        double normSq = 0;
        int end = offset + length;
        for (int j = offset; j < end; j++) {
            double t = v[j];
            normSq += t * t;
        }
        if (normSq == 0) {
            return;
        }
        double invNorm = 1.0 / Math.sqrt(normSq);
        for (int j = offset; j < end; j++) {
            v[j] = (byte) (v[j] * invNorm);
        }
    }
}
