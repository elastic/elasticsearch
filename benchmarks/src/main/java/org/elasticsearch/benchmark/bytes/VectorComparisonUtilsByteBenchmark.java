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
import org.elasticsearch.simdvec.VectorComparisonUtils;
import org.elasticsearch.simdvec.internal.vectorization.DefaultVectorComparisonUtils;
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
import org.openjdk.jmh.infra.Blackhole;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.IntConsumer;

@Warmup(iterations = 4, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Fork(value = 1, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
public class VectorComparisonUtilsByteBenchmark {

    static {
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
    }

    final VectorComparisonUtils defaultVecCmpUtils = DefaultVectorComparisonUtils.INSTANCE;
    final VectorComparisonUtils panamaVecCmpUtils = ESVectorizationProvider.getInstance().getVectorUtilSupport().getVectorComparisonUtils();

    @Param(value = { "64", "127", "128", "4096", "16384", "65536", "1048576" })
    public int size;

    byte[] data;
    byte target;
    Random random = new Random(12345);

    @Setup
    public void setup() {
        data = new byte[size];
        // Fill array with random bytes in range 0..15
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) random.nextInt(16);
        }
        target = (byte) random.nextInt(16);
    }

    @Benchmark
    public void bytePanamaBench(Blackhole blackhole) {
        bytePanamaImpl(blackhole::consume);
    }

    void bytePanamaImpl(IntConsumer consumer) {
        final int len = panamaVecCmpUtils.byteVectorLanes();
        final int bound = VectorComparisonUtils.loopBound(data.length, len);
        int i = 0;
        for (; i < bound; i += len) {
            long mask = panamaVecCmpUtils.equalMask(data, i, target);
            int pos = VectorComparisonUtils.firstSet(mask);
            while (pos >= 0) {
                int idx = i + pos;
                consumer.accept(idx);
                pos = VectorComparisonUtils.nextSet(mask, pos);
            }
        }
        // scalar tail
        for (; i < data.length; i++) {
            if (data[i] == target) {
                consumer.accept(i);
            }
        }
    }

    @Benchmark
    public void byteDefaultBench(Blackhole blackhole) {
        byteDefaultImpl(blackhole::consume);
    }

    void byteDefaultImpl(IntConsumer consumer) {
        final int len = defaultVecCmpUtils.byteVectorLanes();
        final int bound = VectorComparisonUtils.loopBound(data.length, len);
        int i = 0;
        for (; i < bound; i += len) {
            long mask = defaultVecCmpUtils.equalMask(data, i, target);
            int pos = VectorComparisonUtils.firstSet(mask);
            while (pos >= 0) {
                int idx = i + pos;
                consumer.accept(idx);
                pos = VectorComparisonUtils.nextSet(mask, pos);
            }
        }
        // scalar tail
        for (; i < data.length; i++) {
            if (data[i] == target) {
                consumer.accept(i);
            }
        }
    }

    @Benchmark
    public void byteScalarBench(Blackhole blackhole) {
        byteScalarImpl(blackhole::consume);

    }

    void byteScalarImpl(IntConsumer consumer) {
        for (int i = 0; i < data.length; i++) {
            if (data[i] == target) {
                consumer.accept(i);
            }
        }
    }
    /*
    // This allows for straightforward comparison of using the Panama Vector API directly.
    // To ensure that the VectorComparisonUtils indirection does not add overhead - which it does not

    static final jdk.incubator.vector.VectorSpecies<Byte> BS = jdk.incubator.vector.ByteVector.SPECIES_PREFERRED;

    @Benchmark
    public void bytePanamaDirectBenchToLong(Blackhole blackhole) {
        bytePanamaDirectImpltoLong(blackhole::consume);
    }

    void bytePanamaDirectImpltoLong(IntConsumer consumer) {
        final int bound = BS.loopBound(data.length);
        int i = 0;
        for (; i < bound; i += BS.length()) {
            long mask = jdk.incubator.vector.ByteVector.fromArray(BS, data, i).eq(target).toLong();
            int pos = VectorComparisonUtils.firstSet(mask);
            while (pos >= 0) {
                int idx = i + pos;
                consumer.accept(idx);
                pos = VectorComparisonUtils.nextSet(mask, pos);
            }
        }
        // scalar tail
        for (; i < data.length; i++) {
            if (data[i] == target) {
                consumer.accept(i);
            }
        }
    }

    @Benchmark
    public void bytePanamaDirectBench(Blackhole blackhole) {
        bytePanamaDirectImpl(blackhole::consume);
    }

    void bytePanamaDirectImpl(IntConsumer consumer) {
        final int bound = BS.loopBound(data.length);
        int i = 0;
        for (; i < bound; i += BS.length()) {
            var mask = jdk.incubator.vector.ByteVector.fromArray(BS, data, i).eq(target);
            int first;
            while ((first = mask.firstTrue()) < mask.length()) {
                int idx = i + first;
                consumer.accept(idx);
                mask = mask.indexInRange(-1 - first, mask.length());
            }
        }
        // scalar tail
        for (; i < data.length; i++) {
            if (data[i] == target) {
                consumer.accept(i);
            }
        }
    }
    */
}
