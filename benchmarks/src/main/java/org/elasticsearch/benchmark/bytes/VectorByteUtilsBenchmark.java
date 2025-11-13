/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.benchmark.bytes;

import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.simdvec.VectorByteUtils;
import org.elasticsearch.simdvec.internal.vectorization.DefaultVectorByteUtils;
import org.elasticsearch.simdvec.internal.vectorization.ESVectorizationProvider;
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

import java.util.Random;
import java.util.concurrent.TimeUnit;

@Warmup(iterations = 4, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Fork(value = 1)
public class VectorByteUtilsBenchmark {

    static {
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
    }

    final VectorByteUtils defDefaultVectorByteUtils = DefaultVectorByteUtils.INSTANCE;
    final VectorByteUtils vectorByteUtils = ESVectorizationProvider.getInstance().getVectorUtilSupport().getVectorByteUtils();

    @Param(value = { "64", "127", "128", "4096", "16384", "65536", "1048576" })
    public int size;

    byte[] data;
    byte target;

    @Setup
    public void setup() {
        Random random = new Random();
        data = new byte[size];

        // Fill array with random bytes in range 0..7
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) random.nextInt(8);
        }

        target = (byte) random.nextInt(8);
    }

    @Benchmark
    @Fork(jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
    public int panamaBench() {
        int ret = 0;
        final int len = vectorByteUtils.vectorLength();
        final int bound = VectorByteUtils.loopBound(data.length, len);
        int i = 0;
        for (; i < bound; i += len) {
            long mask = vectorByteUtils.equalMask(data, i, target);
            int pos = VectorByteUtils.firstSet(mask);
            while (pos >= 0) {
                int idx = i + pos;
                ret += idx;
                pos = VectorByteUtils.nextSet(mask, pos);
            }
        }
        // scalar tail
        for (; i < data.length; i++) {
            if (data[i] == target) {
                ret += i;
            }
        }
        return ret;
    }

    @Benchmark
    public int defaultBench() {
        int ret = 0;
        final int len = defDefaultVectorByteUtils.vectorLength();
        final int bound = VectorByteUtils.loopBound(data.length, len);
        int i = 0;
        for (; i < bound; i += len) {
            long mask = defDefaultVectorByteUtils.equalMask(data, i, target);
            int pos = VectorByteUtils.firstSet(mask);
            while (pos >= 0) {
                int idx = i + pos;
                ret += idx;
                pos = VectorByteUtils.nextSet(mask, pos);
            }
        }
        // scalar tail
        for (; i < data.length; i++) {
            if (data[i] == target) {
                ret += i;
            }
        }
        return ret;
    }

    @Benchmark
    public int scalarBench() {
        int ret = 0;
        for (int i = 0; i < data.length; i++) {
            if (data[i] == target) {
                ret += i;
            }
        }
        return ret;
    }

    // This allows for straightforward comparison of using the Panama Vector API directly.
    // To ensure that the VectorByteUtils indirection does not add overhead - which it does not
    /*
    static final jdk.incubator.vector.VectorSpecies<Byte> BS = jdk.incubator.vector.ByteVector.SPECIES_PREFERRED;

    @Benchmark
    @Fork(jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
    public int panamaDirectBench() {
        int ret = 0;
        final int bound = BS.loopBound(data.length);
        int i = 0;
        for (; i < bound; i += BS.length()) {
            long mask = jdk.incubator.vector.ByteVector.fromArray(BS, data, i).eq(target).toLong();
            int pos = VectorByteUtils.firstSet(mask);
            while (pos >= 0) {
                int idx = i + pos;
                ret += idx;
                pos = VectorByteUtils.nextSet(mask, pos);
            }
        }
        // scalar tail
        for (; i < data.length; i++) {
            if (data[i] == target) {
                ret += i;
            }
        }
        return ret;
    }
    */
}
