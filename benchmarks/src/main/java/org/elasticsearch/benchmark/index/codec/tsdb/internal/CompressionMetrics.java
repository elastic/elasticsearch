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
 * Inject into {@code @Benchmark} methods only; use {@link MetricsConfig} for setup.
 */
@AuxCounters(AuxCounters.Type.EVENTS)
@State(Scope.Thread)
public class CompressionMetrics {

    private static final int BITS_PER_BYTE = 8;

    /** Bytes written per value after encoding. Lower is better. */
    public double encodedBytesPerValue;

    /** Ratio of raw size (8 bytes/value) to encoded size. Higher is better. */
    public double compressionRatio;

    /** Bits used per value after encoding. Compare against nominal input. */
    public double encodedBitsPerValue;

    /** Ratio of encoded size to theoretical minimum. 1.0 = optimal. */
    public double overheadRatio;

    /** Total bytes encoded/decoded in this iteration. */
    public long totalEncodedBytes;

    /** Total values processed in this iteration. */
    public long totalValuesProcessed;

    private MetricsConfig config;

    @Setup(Level.Iteration)
    public void setupIteration() {
        encodedBytesPerValue = 0;
        compressionRatio = 0;
        encodedBitsPerValue = 0;
        overheadRatio = 0;
        totalEncodedBytes = 0;
        totalValuesProcessed = 0;
        config = null;
    }

    public void recordOperation(MetricsConfig config) {
        if (this.config == null) {
            this.config = config;
        }
        totalEncodedBytes += config.getEncodedBytesPerBlock();
        totalValuesProcessed += config.getBlockSize();
    }

    @TearDown(Level.Iteration)
    public void computeMetrics() {
        if (config == null || config.getBlockSize() == 0 || config.getEncodedBytesPerBlock() == 0) {
            return;
        }

        int blockSize = config.getBlockSize();
        int encodedBytes = config.getEncodedBytesPerBlock();
        int nominalBits = config.getNominalBitsPerValue();

        long rawBytes = (long) blockSize * Long.BYTES;
        long theoreticalMin = ceilDiv((long) blockSize * nominalBits, BITS_PER_BYTE);

        encodedBytesPerValue = (double) encodedBytes / blockSize;
        compressionRatio = (double) rawBytes / encodedBytes;
        encodedBitsPerValue = encodedBytesPerValue * BITS_PER_BYTE;
        overheadRatio = theoreticalMin > 0 ? (double) encodedBytes / theoreticalMin : 0;
    }

    private static long ceilDiv(long dividend, int divisor) {
        return (dividend + divisor - 1) / divisor;
    }
}
