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

/**
 * JMH auxiliary counters for tracking throughput in TSDB codec benchmarks.
 *
 * <p>This class uses JMH's {@link AuxCounters} with {@link AuxCounters.Type#OPERATIONS}
 * to report throughput metrics normalized by time. JMH automatically divides accumulated
 * values by benchmark time to produce rate metrics (e.g., bytes/s, values/s).
 *
 * <p><strong>Important:</strong> {@link AuxCounters.Type#OPERATIONS} only works with
 * {@link org.openjdk.jmh.annotations.Mode#Throughput} or
 * {@link org.openjdk.jmh.annotations.Mode#AverageTime}. It does NOT work with
 * {@link org.openjdk.jmh.annotations.Mode#SampleTime}.
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * @BenchmarkMode(Mode.Throughput)
 * @Benchmark
 * public void benchmark(Blackhole bh, ThroughputMetrics metrics) {
 *     encode.benchmark(bh);
 *     metrics.recordOperation(encode.getBlockSize(), encode.getEncodedSize());
 * }
 * }</pre>
 */
@AuxCounters(AuxCounters.Type.OPERATIONS)
@State(Scope.Thread)
public class ThroughputMetrics {

    /** Bytes encoded or decoded. JMH normalizes by time to produce bytes/s. */
    public long encodedBytes;

    /** Values processed. JMH normalizes by time to produce values/s. */
    public long valuesProcessed;

    /**
     * Resets all metrics at the start of each iteration.
     */
    @Setup(Level.Iteration)
    public void setupIteration() {
        encodedBytes = 0;
        valuesProcessed = 0;
    }

    /**
     * Records throughput data for a single benchmark operation.
     *
     * @param blockSize number of values processed in this operation
     * @param bytes number of bytes produced or consumed
     */
    public void recordOperation(int blockSize, int bytes) {
        encodedBytes += bytes;
        valuesProcessed += blockSize;
    }
}
