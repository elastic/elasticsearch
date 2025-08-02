/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.common.util.concurrent;

import org.elasticsearch.common.util.concurrent.TaskExecutionTimeTrackingEsThreadPoolExecutor;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.infra.Blackhole;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

@Threads(Threads.MAX)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.MINUTES)
@State(Scope.Benchmark)
@Fork(1)
public class ThreadPoolUtilizationBenchmark {

    @Param({ "10000" })
    private int callIntervalTicks;

    /**
     * This makes very little difference, all the overhead is in the synchronization
     */
    @Param({ "1000" })
    private int frameDurationMs;

    @Param({ "10000" })
    private int reportingDurationMs;

    private TaskExecutionTimeTrackingEsThreadPoolExecutor.FramedTimeTracker timeTracker;

    @Setup
    public void setup() {
        timeTracker = new TaskExecutionTimeTrackingEsThreadPoolExecutor.FramedTimeTracker(
            Duration.ofMillis(reportingDurationMs).toNanos(),
            Duration.ofMillis(frameDurationMs).toNanos(),
            System::nanoTime
        );
    }

    @Benchmark
    public void baseline() {
        Blackhole.consumeCPU(callIntervalTicks);
    }

    @Group("StartAndEnd")
    @Benchmark
    public void startAndStopTasks() {
        timeTracker.startTask();
        Blackhole.consumeCPU(callIntervalTicks);
        timeTracker.endTask();
    }
}
