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
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Warmup(iterations = 1, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 1, time = 10, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@State(Scope.Thread)
public class ThreadPoolUtilizationBenchmark {

    @Param({ "false", "true" })
    private boolean trackUtilization;

    @Param({ "4", "8", "16" })
    private int poolSize;

    @Param({ "1000000" })
    private int tasksNum;

    @Param({ "10" }) // 10ms is aggressive interval, it increases frame updates on FramedTimeTracker, normally we run at 30/60
    // seconds
    private int utilizationIntervalMs;

    private EsThreadPoolExecutor executor;

    private EsThreadPoolExecutor newExecutor(boolean tracking) {
        var conf = EsExecutors.TaskTrackingConfig.builder();
        if (tracking) {
            conf.trackExecutionTime(0.3).trackUtilization(Duration.ofMillis(utilizationIntervalMs));
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

    @Setup
    public void setup() {
        if (trackUtilization) {
            var exec = newExecutor(true);
            if (exec instanceof TaskExecutionTimeTrackingEsThreadPoolExecutor trackingExecutor) {
                if (trackingExecutor.trackingConfig().trackUtilization() == false) {
                    throw new IllegalStateException("utilization tracking must be enabled");
                } else {
                    executor = trackingExecutor;
                }
            } else {
                throw new IllegalStateException("must be tracking executor");
            }
        } else {
            var exec = newExecutor(false);
            if (exec instanceof TaskExecutionTimeTrackingEsThreadPoolExecutor) {
                throw new IllegalStateException("must be non-tracking executor");
            }
            executor = exec;
        }
    }

    @TearDown
    public void tearDown() throws InterruptedException {
        executor.shutdown();
        executor.awaitTermination(0, TimeUnit.MILLISECONDS);
    }

    @Benchmark
    public void run(Blackhole bh) throws InterruptedException {
        var completedTasks = new CountDownLatch(tasksNum);
        for (var i = 0; i < tasksNum; i++) {
            executor.execute(() -> {
                // busy cycles for cpu
                var r = 0;
                for (var j = 0; j < 1000; j++) {
                    r += j * 2;
                }
                bh.consume(r);
                completedTasks.countDown();
            });
        }
        completedTasks.await();
    }
}
