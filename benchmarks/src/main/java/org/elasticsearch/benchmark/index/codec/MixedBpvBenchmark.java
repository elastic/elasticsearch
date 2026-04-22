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
 * Benchmarks {@link DocValuesForUtil} encode and decode over a stream of blocks whose
 * bits-per-value varies randomly within a small set of values.
 *
 * <p>Real time-series data rarely has a single fixed bpv across all chunks: adjacent blocks
 * commonly differ by a few bits. This benchmark captures that by pre-building a ring of 1 024
 * blocks whose bpv is drawn uniformly at random from the {@code bpvSet} parameter, then
 * rotating through them one block per JMH invocation.
 *
 * <p>The {@code bpvSet} parameter covers representative clusters across the four
 * {@link DocValuesForUtil} dispatch paths:
 * <ul>
 *   <li>{@code "1,2,3"} and {@code "5,10,15"} – tightly clustered small integers
 *       (SimdForUtil expand8 path)</li>
 *   <li>{@code "8,16,24"} – boundaries of the three ForUtil packing lanes</li>
 *   <li>{@code "16,24,32"} – ForUtil → DocValues expand32 transition</li>
 *   <li>{@code "32,40,48"} – DocValues expand32 and multi-byte paths</li>
 * </ul>
 *
 * <p>Scalar vs SIMD is compared via the same {@code @Fork} trick used in the other benchmarks:
 * the {@code simd*} methods add {@code --add-modules=jdk.incubator.vector}, which causes
 * {@code ESVectorUtil} to load {@code PanamaESVectorUtilSupport} instead of the scalar fallback.
 */
@Warmup(iterations = 4, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Fork(value = 1)
public class MixedBpvBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    @Param({ "1,2,3", "5,10,15", "8,16,24", "16,24,32", "32,40,48" })
    public String bpvSet;

    private static final int NUM_BLOCKS = 1024;
    private static final int BLOCK_SIZE = ForUtil.BLOCK_SIZE;

    private DocValuesForUtil docValuesForUtil;

    /** bpv assigned to each pre-built block */
    private int[] bpvPerBlock;

    /** pre-encoded bytes for the decode benchmark (read-only) */
    private byte[][] encodedBlocks;

    /** input arrays for the encode benchmark (modified in-place by collapse, rotated) */
    private long[][] encodeInputs;

    private long[] decodeOutput;
    private byte[] outputBuffer;
    private ByteArrayDataInput dataInput;
    private ByteArrayDataOutput dataOutput;
    private int blockIndex;

    @Setup
    public void setup() throws IOException {
        docValuesForUtil = new DocValuesForUtil(BLOCK_SIZE);

        int[] bpvValues = parseBpvSet(bpvSet);
        Random random = new Random(42);

        bpvPerBlock = new int[NUM_BLOCKS];
        encodedBlocks = new byte[NUM_BLOCKS][];
        encodeInputs = new long[NUM_BLOCKS][];

        byte[] tempBuffer = new byte[BLOCK_SIZE * Long.BYTES];

        for (int b = 0; b < NUM_BLOCKS; b++) {
            int bpv = bpvValues[random.nextInt(bpvValues.length)];
            bpvPerBlock[b] = bpv;

            long maxValue = bpv == 64 ? Long.MAX_VALUE : (1L << bpv) - 1;
            long[] input = new long[BLOCK_SIZE];
            for (int i = 0; i < BLOCK_SIZE; i++) {
                input[i] = random.nextLong() & maxValue;
            }

            // Keep a fresh copy for the encode pool; encode one copy to produce valid bytes.
            encodeInputs[b] = Arrays.copyOf(input, input.length);

            ByteArrayDataOutput encodeOut = new ByteArrayDataOutput(tempBuffer);
            long[] encodeCopy = Arrays.copyOf(input, input.length);
            docValuesForUtil.encode(encodeCopy, bpv, encodeOut);
            encodedBlocks[b] = Arrays.copyOf(tempBuffer, encodeOut.getPosition());
        }

        decodeOutput = new long[BLOCK_SIZE];
        outputBuffer = new byte[BLOCK_SIZE * Long.BYTES];
        dataInput = new ByteArrayDataInput(encodedBlocks[0]);
        dataOutput = new ByteArrayDataOutput(outputBuffer);
        blockIndex = 0;
    }

    // -----------------------------------------------------------------------------------------
    // Decode benchmarks
    // -----------------------------------------------------------------------------------------

    @Benchmark
    public long decode() throws IOException {
        int idx = blockIndex++ & (NUM_BLOCKS - 1);
        dataInput.reset(encodedBlocks[idx]);
        docValuesForUtil.decode(bpvPerBlock[idx], dataInput, decodeOutput);
        return decodeOutput[0];
    }

    @Benchmark
    @Fork(jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
    public long simdDecode() throws IOException {
        int idx = blockIndex++ & (NUM_BLOCKS - 1);
        dataInput.reset(encodedBlocks[idx]);
        docValuesForUtil.decode(bpvPerBlock[idx], dataInput, decodeOutput);
        return decodeOutput[0];
    }

    // -----------------------------------------------------------------------------------------
    // Encode benchmarks
    // -----------------------------------------------------------------------------------------

    @Benchmark
    public int encode() throws IOException {
        int idx = blockIndex++ & (NUM_BLOCKS - 1);
        dataOutput.reset(outputBuffer);
        docValuesForUtil.encode(encodeInputs[idx], bpvPerBlock[idx], dataOutput);
        return dataOutput.getPosition();
    }

    @Benchmark
    @Fork(jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
    public int simdEncode() throws IOException {
        int idx = blockIndex++ & (NUM_BLOCKS - 1);
        dataOutput.reset(outputBuffer);
        docValuesForUtil.encode(encodeInputs[idx], bpvPerBlock[idx], dataOutput);
        return dataOutput.getPosition();
    }

    // -----------------------------------------------------------------------------------------

    private static int[] parseBpvSet(String param) {
        String[] parts = param.split(",");
        int[] values = new int[parts.length];
        for (int i = 0; i < parts.length; i++) {
            values[i] = Integer.parseInt(parts[i].trim());
        }
        return values;
    }
}
