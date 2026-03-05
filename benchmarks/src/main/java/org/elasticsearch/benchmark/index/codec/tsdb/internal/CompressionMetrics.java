/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.tsdb.internal;

import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

/**
 * JMH auxiliary counters for tracking compression efficiency in TSDB codec benchmarks.
 *
 * <p>This class uses JMH's {@link AuxCounters} feature to report compression metrics
 * alongside timing data. Metrics are accumulated during benchmark operations and
 * computed at iteration teardown.
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * @Benchmark
 * public void benchmark(Blackhole bh, CompressionMetrics metrics) {
 *     encode.benchmark(bh);
 *     metrics.recordOperation(encode.getBlockSize, encode.getEncodedSize(), bitsPerValue);
 * }
 * }</pre>
 */
@AuxCounters(AuxCounters.Type.EVENTS)
@State(Scope.Thread)
public class CompressionMetrics {

    /**
     * Average bytes written per value after encoding.
     * Lower values indicate better compression.
     */
    public double encodedBytesPerValue;

    /**
     * Compression ratio: raw size (8 bytes/value) divided by encoded size.
     * Higher values indicate better compression. A ratio of 8.0 means
     * the data was compressed to 1 byte per value.
     */
    public double compressionRatio;

    /**
     * Average bits used per value after encoding.
     * Compare against the nominal input {@code bitsPerValue} to assess
     * compression effectiveness.
     */
    public double encodedBitsPerValue;

    /**
     * Ratio of actual encoded size to theoretical minimum size at nominal bit width.
     * <ul>
     *   <li>{@code < 1.0}: encoder exploited patterns to compress below nominal bit width.
     *       This happens when delta encoding, min offset removal, or GCD division reduce
     *       the effective bits per value below the nominal input bit width.</li>
     *   <li>{@code = 1.0}: optimal bit-packing at nominal bit width with no overhead</li>
     *   <li>{@code > 1.0}: metadata overhead exceeds compression gains. This occurs with
     *       incompressible data (e.g., random values) where the encoder cannot exploit
     *       patterns but still writes encoding metadata (tokens, min offset, GCD, etc.).</li>
     * </ul>
     * Lower values are better. Useful for comparing encoder versions on the same data pattern.
     */
    public double overheadRatio;

    /** Number of values in each encoded block. */
    private int blockSize;

    /** Actual bytes produced after encoding one block. */
    private int encodedBytesPerBlock;

    /** The nominal bits per value being tested (benchmark parameter). */
    private int nominalBitsPerValue;

    /**
     * Resets all metrics at the start of each iteration.
     */
    @Setup(Level.Iteration)
    public void setupIteration() {
        encodedBytesPerValue = 0;
        compressionRatio = 0;
        encodedBitsPerValue = 0;
        overheadRatio = 0;
        blockSize = 0;
        encodedBytesPerBlock = 0;
        nominalBitsPerValue = 0;
    }

    /**
     * Records metrics for a single benchmark operation.
     * Call this method at the end of each {@code @Benchmark} method.
     *
     * @param blockSize number of values per encoded block
     * @param encodedBytes actual bytes produced after encoding one block
     * @param nominalBits the nominal bits per value being tested
     */
    public void recordOperation(int blockSize, int encodedBytes, int nominalBits) {
        this.blockSize = blockSize;
        this.encodedBytesPerBlock = encodedBytes;
        this.nominalBitsPerValue = nominalBits;
    }

    /**
     * Computes final compression metrics at the end of each iteration.
     * Called automatically by JMH after all operations in an iteration complete.
     */
    @TearDown(Level.Iteration)
    public void computeMetrics() {
        if (blockSize == 0) {
            return;
        }

        long rawBytes = (long) blockSize * Long.BYTES;
        long theoreticalMin = Math.ceilDiv((long) blockSize * nominalBitsPerValue, Byte.SIZE);

        encodedBytesPerValue = (double) encodedBytesPerBlock / blockSize;
        compressionRatio = (double) rawBytes / encodedBytesPerBlock;
        encodedBitsPerValue = encodedBytesPerValue * Byte.SIZE;
        overheadRatio = theoreticalMin > 0 ? (double) encodedBytesPerBlock / theoreticalMin : 0;
    }
}
