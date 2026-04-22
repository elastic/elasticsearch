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
import org.elasticsearch.index.codec.tsdb.DocValuesForUtil;
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
 * Benchmarks {@link DocValuesForUtil} encode and decode across the full supported range of
 * bits-per-value (1–64), comparing the scalar fallback (no Panama vector module) against the
 * SIMD-accelerated implementation (with {@code --add-modules=jdk.incubator.vector}).
 *
 * <p>DocValuesForUtil dispatches to four different code paths depending on bpv:
 * <ul>
 *   <li>1–24: {@link org.elasticsearch.index.codec.SimdForUtil} (expand8/16/32)</li>
 *   <li>25–32: expand/collapse 32-bit lanes via ESVectorUtil</li>
 *   <li>40/48/56: five/six/seven bytes per value via ESVectorUtil.decodeMultiByteLongs</li>
 *   <li>57–64: raw long reads/writes (no SIMD benefit)</li>
 * </ul>
 *
 * <p>Only rounded bpv values are used because {@link DocValuesForUtil#encode} and
 * {@link DocValuesForUtil#decode} require the caller to pass values already rounded via
 * {@link DocValuesForUtil#roundBits}.
 */
@Warmup(iterations = 4, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Fork(value = 1)
public class DocValuesForUtilBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    /**
     * Representative rounded bpv values covering every DocValuesForUtil dispatch path:
     * 1–24 (SimdForUtil), 32 (expand32), 40/48/56 (multi-byte), 64 (raw longs).
     */
    @Param({ "1", "8", "16", "24", "32", "40", "48", "56", "64" })
    public int bitsPerValue;

    private static final int BLOCK_SIZE = ForUtil.BLOCK_SIZE;
    private static final int POOL_SIZE = 256;

    private DocValuesForUtil docValuesForUtil;

    // Encode pool: encode modifies the input array in-place (collapse32), so we rotate through
    // copies. Performance is value-independent (pure bit-shifts/masks), so dirty copies are fine.
    private long[][] encodeInputPool;
    private int encodeIndex;

    private long[] decodeOutput;
    private byte[] encodedBytes;
    private byte[] outputBuffer;
    private ByteArrayDataInput dataInput;
    private ByteArrayDataOutput dataOutput;

    @Setup
    public void setup() throws IOException {
        docValuesForUtil = new DocValuesForUtil(BLOCK_SIZE);

        Random random = new Random(42);
        long maxValue = bitsPerValue == 64 ? Long.MAX_VALUE : (1L << bitsPerValue) - 1;

        long[] original = new long[BLOCK_SIZE];
        for (int i = 0; i < BLOCK_SIZE; i++) {
            original[i] = random.nextLong() & maxValue;
        }

        encodeInputPool = new long[POOL_SIZE][];
        for (int i = 0; i < POOL_SIZE; i++) {
            encodeInputPool[i] = Arrays.copyOf(original, original.length);
        }
        encodeIndex = 0;

        // Allocate enough for the widest possible encoding (bpv=64 → 128 raw longs = 1024 bytes).
        outputBuffer = new byte[BLOCK_SIZE * Long.BYTES];

        // Pre-encode once (using a fresh copy) to produce valid bytes for the decode benchmarks.
        long[] encodeCopy = Arrays.copyOf(original, original.length);
        ByteArrayDataOutput encodeOut = new ByteArrayDataOutput(outputBuffer);
        docValuesForUtil.encode(encodeCopy, bitsPerValue, encodeOut);
        encodedBytes = Arrays.copyOf(outputBuffer, encodeOut.getPosition());

        decodeOutput = new long[BLOCK_SIZE];
        dataInput = new ByteArrayDataInput(encodedBytes);
        dataOutput = new ByteArrayDataOutput(outputBuffer);
    }

    // -----------------------------------------------------------------------------------------
    // Decode benchmarks
    // -----------------------------------------------------------------------------------------

    @Benchmark
    public long decode() throws IOException {
        dataInput.reset(encodedBytes);
        docValuesForUtil.decode(bitsPerValue, dataInput, decodeOutput);
        return decodeOutput[0];
    }

    @Benchmark
    @Fork(jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
    public long simdDecode() throws IOException {
        dataInput.reset(encodedBytes);
        docValuesForUtil.decode(bitsPerValue, dataInput, decodeOutput);
        return decodeOutput[0];
    }

    // -----------------------------------------------------------------------------------------
    // Encode benchmarks
    // -----------------------------------------------------------------------------------------

    @Benchmark
    public int encode() throws IOException {
        dataOutput.reset(outputBuffer);
        docValuesForUtil.encode(encodeInputPool[encodeIndex++ & (POOL_SIZE - 1)], bitsPerValue, dataOutput);
        return dataOutput.getPosition();
    }

    @Benchmark
    @Fork(jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
    public int simdEncode() throws IOException {
        dataOutput.reset(outputBuffer);
        docValuesForUtil.encode(encodeInputPool[encodeIndex++ & (POOL_SIZE - 1)], bitsPerValue, dataOutput);
        return dataOutput.getPosition();
    }
}
