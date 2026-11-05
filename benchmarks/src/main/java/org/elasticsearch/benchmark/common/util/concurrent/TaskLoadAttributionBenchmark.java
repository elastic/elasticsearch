/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.common.util.concurrent;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.common.util.concurrent.TaskLoadAttribution;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * Measures what charging its load costs a task of a pool that attributes task load. Each task spins for {@code workNanos} and
 * charges nothing ({@code NONE}), lets its pool charge a target, as a shard search task does ({@code SHARD_TASK}), or also
 * charges the target the time it spun itself, as an ES|QL driver does ({@code DRIVER}).
 */
@Fork(2)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class TaskLoadAttributionBenchmark {

    private static final int TASKS = 1024;

    public enum Charging {
        NONE,
        SHARD_TASK,
        DRIVER
    }

    @Param({ "0", "2000", "10000", "100000" })
    long workNanos;

    @Param({ "NONE", "SHARD_TASK", "DRIVER" })
    Charging charging;

    private EsThreadPoolExecutor pool;
    private final LongAdder charged = new LongAdder();
    private TaskLoadAttribution.Target target;

    @Setup(Level.Trial)
    public void setup() {
        target = (nanos, cpuNanos, finishNanos) -> charged.add(cpuNanos);
        pool = EsExecutors.newFixed(
            "attributing",
            1,
            -1,
            EsExecutors.daemonThreadFactory("bench", "attributing"),
            new ThreadContext(Settings.EMPTY),
            EsExecutors.TaskTrackingConfig.builder().trackExecutionTime(0.3).attributeTaskLoad().build()
        );
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        pool.shutdownNow();
    }

    @Benchmark
    @OperationsPerInvocation(TASKS)
    public long runTasks() throws InterruptedException {
        CountDownLatch done = new CountDownLatch(TASKS);
        for (int i = 0; i < TASKS; i++) {
            pool.execute(() -> {
                if (charging != Charging.NONE) {
                    TaskLoadAttribution.attribute(target);
                }
                long start = System.nanoTime();
                while (System.nanoTime() - start < workNanos) {
                    Thread.onSpinWait();
                }
                if (charging == Charging.DRIVER) {
                    long now = System.nanoTime();
                    TaskLoadAttribution.charge(target, now - start, now);
                }
                done.countDown();
            });
        }
        done.await();
        return charged.sum();
    }
}
