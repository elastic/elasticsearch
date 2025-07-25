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
import org.elasticsearch.common.util.concurrent.TaskExecutionTimeTrackingEsThreadPoolExecutor;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Warmup(iterations = 0)
@Measurement(iterations = 1, time = 10, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@State(Scope.Thread)
public class ThreadPoolUtilizationBenchmark {
    private final Random random = new Random();
    @Param({ "8", "16", "32", "64" })
    private int poolSize;
    @Param({ "10000" })
    private int tasksNum;
    @Param({ "1" })
    private int workerTimeMinMs;
    @Param({ "5" })
    private int workerTimeMaxMs;
    private TaskExecutionTimeTrackingEsThreadPoolExecutor executor;

    public EsThreadPoolExecutor newExecutor(boolean tracking) {
        var conf = EsExecutors.TaskTrackingConfig.builder();
        if (tracking) {
            conf.trackOngoingTasks().trackUtilization();
        }
        return EsExecutors.newFixed(
            "bench",
            poolSize,
            tasksNum,
            Executors.defaultThreadFactory(),
            new ThreadContext(Settings.EMPTY),
            conf.build()
        );
    }

    private void runTasks(EsThreadPoolExecutor executor, Blackhole bh) throws InterruptedException {
        try {
            var completedTasks = new CountDownLatch(tasksNum);
            for (var i = 0; i < tasksNum; i++) {
                executor.execute(() -> {
                    try {
                        Thread.sleep(random.nextInt(workerTimeMinMs, workerTimeMaxMs));
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    } finally {
                        completedTasks.countDown();
                    }
                });
            }
            completedTasks.await();
        } finally {
            executor.shutdown();
            executor.awaitTermination(0, TimeUnit.MILLISECONDS);
        }
    }

    @Benchmark
    public void trackingExecutor(Blackhole bh) throws InterruptedException {
        runTasks(newExecutor(true), bh);
    }

    @Benchmark
    public void nonTrackingExecutor(Blackhole bh) throws InterruptedException {
        runTasks(newExecutor(false), bh);
    }
}
