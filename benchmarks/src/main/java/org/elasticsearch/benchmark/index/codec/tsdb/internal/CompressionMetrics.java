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
 * public void benchmark(Blackhole bh, MetricsConfig config, CompressionMetrics metrics) {
 *     // ... benchmark-specific code ...
 *     metrics.recordOperation(config);
 * }
 * }</pre>
 *
 * <p>Inject this class into {@code @Benchmark} methods only. For setup methods,
 * use {@link MetricsConfig} instead to avoid JMH injection conflicts.
 *
 * @see MetricsConfig
 */
@AuxCounters(AuxCounters.Type.EVENTS)
@State(Scope.Thread)
public class CompressionMetrics {

    private static final int BITS_PER_BYTE = 8;

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
     * Ratio of actual encoded size to theoretical minimum size.
     * A value of 1.0 indicates optimal encoding with no overhead.
     * Values greater than 1.0 indicate encoding overhead.
     */
    public double overheadRatio;

    /**
     * Total bytes encoded or decoded during this iteration.
     * Accumulated across all operations in the iteration.
     */
    public long totalEncodedBytes;

    /**
     * Total number of values processed during this iteration.
     * Accumulated across all operations in the iteration.
     */
    public long totalValuesProcessed;

    private MetricsConfig config;

    /**
     * Resets all metrics at the start of each iteration.
     */
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

    /**
     * Records metrics for a single benchmark operation.
     * Call this method at the end of each {@code @Benchmark} method.
     *
     * @param config the metrics configuration containing block size and encoded bytes
     */
    public void recordOperation(MetricsConfig config) {
        this.config = config;
        totalEncodedBytes += config.getEncodedSizePerBlock();
        totalValuesProcessed += config.getBlockSize();
    }

    /**
     * Computes final compression metrics at the end of each iteration.
     * Called automatically by JMH after all operations in an iteration complete.
     */
    @TearDown(Level.Iteration)
    public void computeMetrics() {
        if (config == null || config.getBlockSize() == 0 || config.getEncodedSizePerBlock() == 0) {
            return;
        }

        int blockSize = config.getBlockSize();
        int encodedBytes = config.getEncodedSizePerBlock();
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
