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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

@Threads(12)
@Warmup(iterations = 3, time = 200, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 600, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Fork(1)
public class ThreadPoolUtilizationBenchmark {

    @Param({ "0", "10000", "100000" })
    private int callIntervalTicks;

    /**
     * This makes very little difference, all the overhead is in the synchronization
     */
    @Param({ "10" })
    private int utilizationIntervalMs;

    @State(Scope.Thread)
    public static class TaskState {
        boolean running = false;

        boolean shouldStart() {
            return (running = running == false);
        }
    }

    private TaskExecutionTimeTrackingEsThreadPoolExecutor.FramedTimeTracker timeTracker;

    @Setup
    public void setup() {
        timeTracker = new TaskExecutionTimeTrackingEsThreadPoolExecutor.FramedTimeTracker(
            TimeUnit.MILLISECONDS.toNanos(utilizationIntervalMs),
            System::nanoTime
        );
    }

    @Benchmark
    public void baseline() {
        Blackhole.consumeCPU(callIntervalTicks);
    }

    @Group("ReadAndWrite")
    @Benchmark
    public void startAndStopTasks(TaskState state) {
        Blackhole.consumeCPU(callIntervalTicks);
        if (state.shouldStart()) {
            timeTracker.startTask();
        } else {
            timeTracker.endTask();
        }
    }

    @Benchmark
    @Group("ReadAndWrite")
    public void readPrevious(Blackhole blackhole) {
        Blackhole.consumeCPU(callIntervalTicks);
        blackhole.consume(timeTracker.previousFrameTime());
    }

    @Benchmark
    @Group("JustWrite")
    public void startAndStopTasksOnly(TaskState state) {
        Blackhole.consumeCPU(callIntervalTicks);
        if (state.shouldStart()) {
            timeTracker.startTask();
        } else {
            timeTracker.endTask();
        }
    }
}
