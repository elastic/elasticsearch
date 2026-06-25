/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.common.metrics;

import org.elasticsearch.common.metrics.ExponentiallyWeightedMovingRate;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

/**
 * Compares {@link ExponentiallyWeightedMovingRate#addIncrement} throughput under varying write-thread counts, with one concurrent thread
 * always calling {@link ExponentiallyWeightedMovingRate#getRate} — matching the expected production pattern where a thread pool of size N
 * updates the rate and a single monitoring thread reads it periodically.
 *
 * <p>Each benchmark group pairs N writer threads with 1 reader thread. JMH reports {@code addIncrement} and {@code getRate} latencies
 * separately within each group, so both write throughput degradation and read cost are visible. The {@code numStripes} parameter
 * controls stripe count: {@code 1} is the unstriped baseline where all writers serialise through a single monitor; higher values
 * distribute writes across independent monitors.
 *
 * <p>Run with: {@code ./gradlew :benchmarks:run --args="ExponentiallyWeightedMovingRateBenchmark"}
 */
@Fork(2)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@SuppressWarnings("unused")
public class ExponentiallyWeightedMovingRateBenchmark {

    @Param({ "1", "4", "8", "16" })
    public int numStripes;

    private ExponentiallyWeightedMovingRate ewmr;

    @Setup(Level.Trial)
    public void setup() {
        long now = System.nanoTime();
        double halfLifeNanos = TimeUnit.SECONDS.toNanos(30);
        ewmr = new ExponentiallyWeightedMovingRate(Math.log(2.0) / halfLifeNanos, now, numStripes);
    }

    /**
     * Per-thread monotonically increasing time counter. Each thread advances its own counter independently; when multiple threads share a
     * stripe the EWMR's per-stripe time clamping handles any apparent backwards movement from that stripe's perspective.
     */
    @State(Scope.Thread)
    public static class ThreadState {
        long time;

        @Setup(Level.Trial)
        public void setup() {
            time = System.nanoTime();
        }
    }

    // Each group runs N writer threads and 1 reader thread concurrently. JMH reports them separately as
    // "writers_NNN:addIncrement_NNN" and "writers_NNN:getRate_NNN" so write and read latencies are both visible.

    @Group("writers_001")
    @GroupThreads(1)
    @Benchmark
    public void addIncrement_001(ThreadState t) {
        ewmr.addIncrement(1.0, t.time++);
    }

    @Group("writers_001")
    @GroupThreads(1)
    @Benchmark
    public double getRate_001(ThreadState t) {
        return ewmr.getRate(t.time);
    }

    @Group("writers_004")
    @GroupThreads(4)
    @Benchmark
    public void addIncrement_004(ThreadState t) {
        ewmr.addIncrement(1.0, t.time++);
    }

    @Group("writers_004")
    @GroupThreads(1)
    @Benchmark
    public double getRate_004(ThreadState t) {
        return ewmr.getRate(t.time);
    }

    @Group("writers_008")
    @GroupThreads(8)
    @Benchmark
    public void addIncrement_008(ThreadState t) {
        ewmr.addIncrement(1.0, t.time++);
    }

    @Group("writers_008")
    @GroupThreads(1)
    @Benchmark
    public double getRate_008(ThreadState t) {
        return ewmr.getRate(t.time);
    }

    @Group("writers_015")
    @GroupThreads(15)
    @Benchmark
    public void addIncrement_015(ThreadState t) {
        ewmr.addIncrement(1.0, t.time++);
    }

    @Group("writers_015")
    @GroupThreads(1)
    @Benchmark
    public double getRate_015(ThreadState t) {
        return ewmr.getRate(t.time);
    }
}
