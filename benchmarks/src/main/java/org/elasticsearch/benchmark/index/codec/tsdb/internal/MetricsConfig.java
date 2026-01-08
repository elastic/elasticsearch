/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.tsdb.internal;

import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

/**
 * Configuration holder for compression metrics in TSDB codec benchmarks.
 *
 * <p>This class stores encoding parameters that are set during benchmark setup
 * and used by {@link CompressionMetrics} to compute compression statistics.
 * It is annotated with {@code @State(Scope.Benchmark)} to share configuration
 * across all threads in a benchmark.
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * @Setup(Level.Iteration)
 * public void setupIteration(MetricsConfig config) {
 *     // ... setup encoder and run once to measure encoded size ...
 *     config.configure(BLOCK_SIZE, encoder.getEncodedSize(), bitsPerValue);
 * }
 * }</pre>
 *
 * <p>Inject this class into {@code @Setup} methods. For {@code @Benchmark} methods,
 * use {@link CompressionMetrics} instead.
 *
 * @see CompressionMetrics
 */
@State(Scope.Benchmark)
public class MetricsConfig {

    /*
     * These fields are volatile to ensure visibility across threads.
     *
     * JMH lifecycle with Scope.Benchmark:
     * 1. A single thread writes these fields during @Setup(Level.Iteration)
     * 2. Multiple benchmark threads read these fields during @Benchmark execution
     * 3. No concurrent writes occur - setup completes before benchmark threads start
     *
     * Volatile guarantees that writes by the setup thread are visible to all reader threads
     * (happens-before relationship), without needing synchronization since there is no
     * write contention.
     */
    private volatile int blockSize;
    private volatile int encodedBytesPerBlock;
    private volatile int nominalBitsPerValue;

    /**
     * Configures the metrics parameters for the current benchmark iteration.
     *
     * @param blockSize            number of values per encoded block (typically 128)
     * @param encodedBytesPerBlock actual bytes produced after encoding one block
     * @param nominalBitsPerValue  the input {@code bitsPerValue} parameter being tested
     */
    public void configure(int blockSize, int encodedBytesPerBlock, int nominalBitsPerValue) {
        this.blockSize = blockSize;
        this.encodedBytesPerBlock = encodedBytesPerBlock;
        this.nominalBitsPerValue = nominalBitsPerValue;
    }

    /**
     * Returns the number of values per encoded block.
     */
    public int getBlockSize() {
        return blockSize;
    }

    /**
     * Returns the actual bytes produced after encoding one block.
     */
    public int getEncodedSizePerBlock() {
        return encodedBytesPerBlock;
    }

    /**
     * Returns the nominal bits per value being tested.
     */
    public int getNominalBitsPerValue() {
        return nominalBitsPerValue;
    }
}
