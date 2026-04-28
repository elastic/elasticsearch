/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.index.codec.ForUtil;
import org.elasticsearch.index.codec.SimdForUtil;
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

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Compares scalar {@link ForUtil} against SIMD-accelerated {@link SimdForUtil} for encode and
 * decode across every supported bits-per-value (1–24).
 *
 * <p>Run the scalar benchmarks with the standard JVM, and the SIMD benchmarks with
 * {@code --add-modules=jdk.incubator.vector} (applied automatically via the {@code @Fork}
 * annotation on each {@code simd*} method).
 */
@Warmup(iterations = 4, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Fork(value = 1)
public class ForUtilBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    /**
     * ForUtil/SimdForUtil optimise bpv 1–24 with dedicated expand/collapse paths.
     * Values outside that range use a generic slow path and show no SIMD benefit.
     */
    @Param(
        {
            "1",
            "2",
            "3",
            "4",
            "5",
            "6",
            "7",
            "8",
            "9",
            "10",
            "11",
            "12",
            "13",
            "14",
            "15",
            "16",
            "17",
            "18",
            "19",
            "20",
            "21",
            "22",
            "23",
            "24" }
    )
    public int bitsPerValue;

    // Encode consumes (collapses) the input array in-place, so we keep a pool of copies and
    // rotate through them. After one full cycle the copies are dirty but encode is value-
    // independent in terms of performance (pure bit-shifts and masks), so measurements stay valid.
    private static final int POOL_SIZE = 256;
    private long[][] encodeInputPool;
    private int encodeIndex;

    private long[] decodeOutput;
    private byte[] encodedBytes;
    private byte[] outputBuffer;
    private ByteArrayDataInput dataInput;
    private ByteArrayDataOutput dataOutput;

    @Setup
    public void setup() throws IOException {
        Random random = new Random(42);
        long maxValue = (1L << bitsPerValue) - 1;

        long[] original = new long[ForUtil.BLOCK_SIZE];
        for (int i = 0; i < ForUtil.BLOCK_SIZE; i++) {
            original[i] = random.nextLong() & maxValue;
        }

        encodeInputPool = new long[POOL_SIZE][];
        for (int i = 0; i < POOL_SIZE; i++) {
            encodeInputPool[i] = Arrays.copyOf(original, original.length);
        }
        encodeIndex = 0;

        decodeOutput = new long[ForUtil.BLOCK_SIZE];
        outputBuffer = new byte[ForUtil.numBytes(bitsPerValue)];

        // Pre-encode once (using a fresh copy) so decode benchmarks have valid input bytes.
        long[] encodeCopy = Arrays.copyOf(original, original.length);
        ByteArrayDataOutput encodeOut = new ByteArrayDataOutput(outputBuffer);
        ForUtil.encode(encodeCopy, bitsPerValue, encodeOut);
        encodedBytes = Arrays.copyOf(outputBuffer, encodeOut.getPosition());

        dataInput = new ByteArrayDataInput(encodedBytes);
        dataOutput = new ByteArrayDataOutput(outputBuffer);
    }

    // -----------------------------------------------------------------------------------------
    // Decode benchmarks
    // -----------------------------------------------------------------------------------------

    @Benchmark
    public long forUtilDecode() throws IOException {
        dataInput.reset(encodedBytes);
        ForUtil.decode(bitsPerValue, dataInput, decodeOutput);
        return decodeOutput[0];
    }

    @Benchmark
    @Fork(jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
    public long simdForUtilDecode() throws IOException {
        dataInput.reset(encodedBytes);
        SimdForUtil.decode(bitsPerValue, dataInput, decodeOutput);
        return decodeOutput[0];
    }

    // -----------------------------------------------------------------------------------------
    // Encode benchmarks
    // -----------------------------------------------------------------------------------------

    @Benchmark
    public int forUtilEncode() throws IOException {
        dataOutput.reset(outputBuffer);
        ForUtil.encode(encodeInputPool[encodeIndex++ & (POOL_SIZE - 1)], bitsPerValue, dataOutput);
        return dataOutput.getPosition();
    }

    @Benchmark
    @Fork(jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
    public int simdForUtilEncode() throws IOException {
        dataOutput.reset(outputBuffer);
        SimdForUtil.encode(encodeInputPool[encodeIndex++ & (POOL_SIZE - 1)], bitsPerValue, dataOutput);
        return dataOutput.getPosition();
    }
}
