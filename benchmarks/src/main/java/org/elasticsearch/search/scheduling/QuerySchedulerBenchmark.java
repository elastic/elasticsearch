/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.scheduling;

import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.search.stats.SearchStatsSettings;
import org.elasticsearch.index.search.stats.ShardSearchStats;
import org.elasticsearch.tasks.TaskId;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/**
 * Compares submitting search tasks to a pool directly with submitting them through {@link QueryScheduler}, on a fixed pool
 * built like {@code search}, with real {@link ShardSearchStats} accumulating the waiting. Each operation submits a batch
 * of {@link #tasks} tasks from one thread: {@code submit} times the submissions only, {@code complete} times until every
 * task completed and the scheduler no longer tracks it. Results are per batch. Run with {@code -prof gc} for the bytes
 * allocated per batch.
 */
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@SuppressWarnings("unused") // invoked by benchmarking framework
public class QuerySchedulerBenchmark {

    static {
        BenchmarkLogging.configure();
    }

    private static final int MAX_SHARDS = 100;
    private static final BooleanSupplier HAS_WORK = () -> true;

    /** Tasks per batch. With one query and one shard per executor, they are as many DSL shard phases of a query. */
    @Param({ "64", "1024" })
    private int tasks;

    /** {@code direct} submits to the pool, {@code scheduler} through {@link QueryScheduler}, as the engines do. */
    @Param({ "direct", "scheduler" })
    private String mode;

    /**
     * Threads of the pool: 64 runs many tasks at once, 4 queues most of them. 0 runs each task inline on the submitting
     * thread, so the time is what the scheduler adds to the tasks, without handing them to another thread.
     */
    @Param({ "0", "64", "4" })
    private int threads;

    /** Work per task in {@link Blackhole#consumeCPU} tokens: 0 exposes the bookkeeping, 10000 is tens of microseconds. */
    @Param({ "0", "10000" })
    private long work;

    /** Queries the tasks of a batch are spread over: 1 shares one query, 64 gives each query a share of the tasks. */
    @Param({ "1", "64" })
    private int queries;

    /**
     * Shards per executor. 1 opens an executor per task for one of {@link #MAX_SHARDS} shards, like a DSL shard phase, so
     * executors of a query share shards when there are more tasks than shards. More opens one executor per query for all
     * its tasks, like an ES|QL shard batch (10 shards by default, 100 at most).
     */
    @Param({ "1", "10", "100" })
    private int shards;

    private ExecutorService pool;
    private QueryScheduler scheduler;
    private ShardSearchStats[] stats;
    private List<QueryScheduler.Shard> batchShards;
    private TaskId[] queryIds;
    private CountDownLatch done;

    @Setup(Level.Trial)
    public void setup() {
        pool = threads == 0
            ? EsExecutors.DIRECT_EXECUTOR_SERVICE
            : EsExecutors.newFixed(
                "search",
                threads,
                threads * 1000,
                EsExecutors.daemonThreadFactory("benchmark", "search"),
                new ThreadContext(Settings.EMPTY),
                EsExecutors.TaskTrackingConfig.builder()
                    .trackOngoingTasks()
                    .trackExecutionTime(EsExecutors.TaskTrackingConfig.DEFAULT_EXECUTION_TIME_EWMA_ALPHA_FOR_TEST)
                    .build()
            );
        scheduler = new QueryScheduler(System::nanoTime);
        SearchStatsSettings settings = new SearchStatsSettings(
            new ClusterSettings(Settings.EMPTY, Set.of(SearchStatsSettings.RECENT_READ_LOAD_HALF_LIFE_SETTING))
        );
        stats = new ShardSearchStats[MAX_SHARDS];
        for (int i = 0; i < stats.length; i++) {
            stats[i] = new ShardSearchStats(settings);
        }
        batchShards = new ArrayList<>(shards);
        for (int i = 0; i < shards; i++) {
            batchShards.add(shard(stats[i]));
        }
        queryIds = new TaskId[queries];
        for (int i = 0; i < queries; i++) {
            queryIds[i] = new TaskId("coordinator", i);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() throws InterruptedException {
        if (threads > 0) {
            pool.shutdown();
            if (pool.awaitTermination(10, TimeUnit.SECONDS) == false) {
                throw new IllegalStateException("pool did not terminate");
            }
        }
        if (scheduler.queries() != 0) {
            throw new IllegalStateException("queries still tracked: " + scheduler.queries());
        }
    }

    /** Waits for the tasks of the last operation, outside the measured time. */
    @TearDown(Level.Invocation)
    public void awaitTasks() throws InterruptedException {
        done.await();
        awaitUntracked();
    }

    @Benchmark
    public void submit() {
        submitAll();
    }

    @Benchmark
    public void complete() throws InterruptedException {
        submitAll().await();
        awaitUntracked();
    }

    /** The tasks count the latch down before the scheduler stops tracking them. */
    private void awaitUntracked() {
        while (scheduler.queries() != 0) {
            Thread.onSpinWait();
        }
    }

    private CountDownLatch submitAll() {
        CountDownLatch latch = new CountDownLatch(tasks);
        done = latch;
        if (mode.equals("direct")) {
            for (int i = 0; i < tasks; i++) {
                pool.execute(task(latch));
            }
        } else if (shards == 1) {
            for (int i = 0; i < tasks; i++) {
                ShardSearchStats shardStats = stats[i % stats.length];
                try (var executor = scheduler.newExecutor(queryIds[i % queries], pool, List.of(shard(shardStats)))) {
                    executor.execute(task(latch));
                }
            }
        } else {
            QueryScheduler.QueryExecutor[] executors = new QueryScheduler.QueryExecutor[queries];
            for (int q = 0; q < queries; q++) {
                executors[q] = scheduler.newExecutor(queryIds[q], pool, batchShards);
            }
            for (int i = 0; i < tasks; i++) {
                executors[i % queries].execute(task(latch));
            }
            for (QueryScheduler.QueryExecutor executor : executors) {
                executor.close();
            }
        }
        return latch;
    }

    /** A shard as {@link QueryScheduler#executor} builds it from a searched shard. */
    private static QueryScheduler.Shard shard(ShardSearchStats shardStats) {
        return new QueryScheduler.Shard(shardStats, shardStats::accumulateSearchWaiting, HAS_WORK);
    }

    private AbstractRunnable task(CountDownLatch latch) {
        return new AbstractRunnable() {
            @Override
            protected void doRun() {
                Blackhole.consumeCPU(work);
            }

            @Override
            public void onAfter() {
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }
        };
    }
}
