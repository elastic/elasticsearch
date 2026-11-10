/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.querycapture;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchCaptureContext;
import org.elasticsearch.action.search.SearchCaptureContextBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.logging.activity.QueryCaptureService;
import org.elasticsearch.common.logging.activity.QueryInterceptor;
import org.elasticsearch.common.logging.activity.QuerySink;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.DefaultBuiltInExecutorBuilders;
import org.elasticsearch.threadpool.ThreadPool;
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
import org.openjdk.jmh.infra.IterationParams;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Measures search interception overhead compared to immediate execution.
 *
 * Each producer owns its request and response: running with eight threads does not introduce artificial contention on one response.
 * Run with {@code -t 1} and {@code -t 8}, and {@code -prof gc} to record allocations as well as latency.
 * Async scenarios share one bounded worker and measure submission, including rejection under saturation, rather than worker completion.
 * Per-iteration pool statistics expose the processed/rejected mix. GC allocations include both producer and worker threads.
 * These are interception costs, not end-to-end search latencies.
 * <p>
 * To measure accepted submissions without saturating the queue, use one producer with
 * {@code -t 1 -bm ss -wi 100 -i 30 -bs 512 -wbs 512}. The queue is drained between bursts outside the timed method.
 * In this mode JMH reports time and allocations per burst; divide by 512 to obtain per-capture values.
 * Confirm zero rejections in the iteration statistics when interpreting this as accepted-submission cost.
 */
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
public class QueryInterceptorBenchmark {
    private static final NamedWriteableRegistry REGISTRY = new NamedWriteableRegistry(List.of());

    @Param({ "direct", "disabled", "metadata", "serialize", "async_disabled", "async_metadata", "async_serialize" })
    public String scenario;

    private SearchRequest request;
    private SearchResponse response;
    private Task task;
    private QueryInterceptor<SearchCaptureContext> interceptor;
    private ActionListener<SearchResponse> listener;
    private Consumer<ActionListener<SearchResponse>> action;
    private SearchResponse delivered;
    private volatile int resultCount;
    private volatile String query;
    private boolean asynchronous;
    private boolean serialize;
    private volatile boolean enabled;

    /** Builds reusable search fixtures and checks that each scenario actually exercises its intended path. */
    @Setup
    public void setup(CaptureWorker worker) throws InterruptedException {
        BenchmarkLogging.configure();
        float[] vector = new float[768];
        Random random = new Random(0);
        for (int i = 0; i < vector.length; i++) {
            vector[i] = random.nextFloat();
        }
        var knn = new KnnSearchBuilder("vector", vector, 10, 100, null, null, null).addFilterQuery(QueryBuilders.termQuery("category", 3));
        request = new SearchRequest("index").source(new SearchSourceBuilder().knnSearch(List.of(knn)));
        response = new SearchResponse(
            SearchHits.EMPTY_WITH_TOTAL_HITS,
            null,
            null,
            false,
            null,
            null,
            1,
            null,
            1,
            1,
            0,
            1,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );
        task = new Task(1, "transport", "indices:data/read/search", "", TaskId.EMPTY_TASK_ID, Map.of());
        asynchronous = scenario.startsWith("async_");
        switch (scenario) {
            case "direct", "disabled", "async_disabled" -> {
                enabled = false;
                serialize = false;
            }
            case "metadata", "async_metadata" -> {
                // Read only the result count; do not serialize the query.
                enabled = true;
                serialize = false;
            }
            case "serialize", "async_serialize" -> {
                enabled = true;
                serialize = true;
            }
            default -> throw new IllegalArgumentException("Unknown scenario: " + scenario);
        }
        resultCount = -1;
        interceptor = new QueryInterceptor<>(List.of(new QuerySink<SearchCaptureContext>(() -> "benchmark") {
            @Override
            public boolean isEnabled() {
                return enabled;
            }

            @Override
            public void accept(SearchCaptureContext context) {
                if (serialize) {
                    query = context.getQuery();
                }
                resultCount = context.getResultCount();
            }
        }));
        listener = ActionListener.wrap(r -> delivered = r, e -> { throw new AssertionError(e); });
        action = l -> l.onResponse(response);
    }

    private void invoke(CaptureWorker worker) {
        if (scenario.equals("direct")) {
            action.accept(listener);
        } else if (asynchronous) {
            interceptor.wrapAndRun(listener, new SearchCaptureContextBuilder(task, REGISTRY, request), action, worker.service);
        } else {
            interceptor.wrapAndRun(listener, new SearchCaptureContextBuilder(task, REGISTRY, request), action);
        }
    }

    /** Measures the work on the search thread, with captured outputs consumed to prevent dead-code elimination. */
    @Benchmark
    public void capture(Blackhole blackhole, CaptureWorker worker) {
        invoke(worker);
        blackhole.consume(delivered);
        blackhole.consume(resultCount);
        blackhole.consume(query);
    }

    /** Releases the fixture's response reference after all invocations finish. */
    @TearDown
    public void teardown() {
        response.decRef();
    }

    /** One node-wide worker, shared by all producers; draining and statistics collection are outside the timed method. */
    @State(Scope.Benchmark)
    public static class CaptureWorker {
        private ThreadPool pool;
        private QueryCaptureService service;
        private EsThreadPoolExecutor executor;
        private long completedBefore;
        private long rejectedBefore;

        @Setup(Level.Trial)
        public void setup() {
            BenchmarkLogging.configure();
            pool = new ThreadPool(
                Settings.builder().put("node.name", "query-capture-benchmark").build(),
                MeterRegistry.NOOP,
                new DefaultBuiltInExecutorBuilders()
            );
            service = new QueryCaptureService(pool);
            executor = (EsThreadPoolExecutor) pool.executor(ThreadPool.Names.QUERY_CAPTURE);
        }

        @Setup(Level.Iteration)
        public void startIteration() throws Exception {
            drain();
            completedBefore = executor.getCompletedTaskCount();
            rejectedBefore = rejected();
        }

        @TearDown(Level.Iteration)
        public void finishIteration(IterationParams iteration) throws Exception {
            drain();
            // Exclude the FIFO barrier submitted by drain().
            long completed = executor.getCompletedTaskCount() - completedBefore - 1;
            long rejected = rejected() - rejectedBefore;
            if (completed + rejected > 0) {
                System.out.printf(
                    "capture pool [%s]: processed=%d rejected=%d dropped=%.2f%%%n",
                    iteration.getType(),
                    completed,
                    rejected,
                    100.0 * rejected / (completed + rejected)
                );
            }
        }

        private long rejected() {
            for (var stats : pool.stats()) {
                if (stats.name().equals(ThreadPool.Names.QUERY_CAPTURE)) {
                    return stats.rejected();
                }
            }
            throw new AssertionError("capture pool missing");
        }

        private void drain() throws Exception {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            // Producers have stopped. Make room for a FIFO barrier without introducing a synthetic rejection.
            while (executor.getQueue().remainingCapacity() == 0) {
                if (System.nanoTime() > deadline) {
                    throw new AssertionError("capture queue did not drain");
                }
                Thread.sleep(1);
            }
            executor.submit(() -> {}).get(30, TimeUnit.SECONDS);
            // Future completion precedes the executor updating its completed-task count.
            while (executor.getActiveCount() != 0) {
                if (System.nanoTime() > deadline) {
                    throw new AssertionError("capture worker did not finish");
                }
                Thread.sleep(1);
            }
        }

        @TearDown(Level.Trial)
        public void teardown() {
            if (ThreadPool.terminate(pool, 30, TimeUnit.SECONDS) == false) {
                throw new AssertionError("capture worker did not terminate");
            }
        }
    }
}
