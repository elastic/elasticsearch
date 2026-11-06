/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.columnar;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.concurrent.TimeUnit;

/**
 * Measures one read of each clock that a timer can use: {@link System#nanoTime()} for elapsed time and
 * {@link ThreadMXBean#getCurrentThreadCpuTime()} for CPU time. Timer bookkeeping and sampling are excluded. The setup
 * fails on a platform that cannot measure the CPU time of the current thread.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Fork(1)
@Threads(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class ProfilingClockBenchmark {

    private ThreadMXBean threads;

    @Setup
    public void setup() {
        threads = ManagementFactory.getThreadMXBean();
        if (threads.isCurrentThreadCpuTimeSupported() == false || threads.isThreadCpuTimeEnabled() == false) {
            throw new IllegalStateException("this platform cannot measure the current thread's CPU time");
        }
    }

    @Benchmark
    public long elapsedClock() {
        return System.nanoTime();
    }

    @Benchmark
    public long threadCpuClock() {
        return threads.getCurrentThreadCpuTime();
    }
}
