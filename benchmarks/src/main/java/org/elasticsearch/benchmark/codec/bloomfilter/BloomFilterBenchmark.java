/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.codec.bloomfilter;

import org.apache.lucene.util.BitUtil;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.simdvec.ESVectorUtil;
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

/**
 * Benchmark comparing scalar vs SIMD implementations for bloom filter operations:
 * popcount (counting set bits) and byte array OR (merging bloom filter pages).
 */
@Warmup(iterations = 4, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Fork(value = 1)
public class BloomFilterBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    @Param({ "128", "4096", "16384" })
    public int pageSize;

    @Param({ "0.1", "0.5", "0.9" })
    public double saturation;

    private static final int NUM_PAGES = 1024;

    private byte[] page;
    private byte[][] sourcePages;
    private byte[][] destPagesScalar;
    private byte[][] destPagesSimd;
    private int orIndex;

    @Setup
    public void setup() {
        Random random = new Random(42);
        page = generatePage(random, pageSize, saturation);
        sourcePages = new byte[NUM_PAGES][];
        destPagesScalar = new byte[NUM_PAGES][];
        destPagesSimd = new byte[NUM_PAGES][];
        for (int i = 0; i < NUM_PAGES; i++) {
            sourcePages[i] = generatePage(random, pageSize, saturation);
            destPagesScalar[i] = generatePage(random, pageSize, saturation);
            destPagesSimd[i] = new byte[pageSize];
            System.arraycopy(destPagesScalar[i], 0, destPagesSimd[i], 0, pageSize);
        }
    }

    @Benchmark
    public long popcountScalar() {
        return scalarPopcount(page, 0, page.length);
    }

    @Benchmark
    @Fork(jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
    public long popcountSimd() {
        return ESVectorUtil.popcount(page, 0, page.length);
    }

    @Benchmark
    public void orScalar() {
        int idx = orIndex++ & (NUM_PAGES - 1);
        scalarOr(sourcePages[idx], destPagesScalar[idx], 0, pageSize);
    }

    @Benchmark
    @Fork(jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
    public void orSimd() {
        int idx = orIndex++ & (NUM_PAGES - 1);
        ESVectorUtil.orByteArrays(sourcePages[idx], destPagesSimd[idx], 0, pageSize);
    }

    static long scalarPopcount(byte[] data, int offset, int length) {
        long cnt = 0;
        int i = offset;
        final int upperBound = offset + (length & -Integer.BYTES);
        for (; i < upperBound; i += Integer.BYTES) {
            cnt += Integer.bitCount((int) BitUtil.VH_NATIVE_INT.get(data, i));
        }
        for (; i < offset + length; i++) {
            cnt += Integer.bitCount(data[i] & 0xFF);
        }
        return cnt;
    }

    static void scalarOr(byte[] source, byte[] dest, int offset, int length) {
        int i = offset;
        final int upperBound = offset + (length & -Long.BYTES);
        for (; i < upperBound; i += Long.BYTES) {
            long s = (long) BitUtil.VH_NATIVE_LONG.get(source, i);
            long d = (long) BitUtil.VH_NATIVE_LONG.get(dest, i);
            BitUtil.VH_NATIVE_LONG.set(dest, i, s | d);
        }
        for (; i < offset + length; i++) {
            dest[i] |= source[i];
        }
    }

    private static byte[] generatePage(Random random, int size, double saturation) {
        byte[] page = new byte[size];
        for (int i = 0; i < size; i++) {
            int bits = 0;
            for (int b = 0; b < 8; b++) {
                if (random.nextDouble() < saturation) {
                    bits |= (1 << b);
                }
            }
            page[i] = (byte) bits;
        }
        return page;
    }
}
