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
import org.elasticsearch.common.logging.activity.QueryInterceptor;
import org.elasticsearch.common.logging.activity.QuerySink;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
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
 * This is the synchronous baseline for a subsequent worker-service comparison, not an end-to-end search benchmark.
 */
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
public class QueryInterceptorBenchmark {
    private static final NamedWriteableRegistry REGISTRY = new NamedWriteableRegistry(List.of());

    @Param({ "direct", "disabled", "metadata", "serialize" })
    public String scenario;

    private SearchRequest request;
    private SearchResponse response;
    private Task task;
    private QueryInterceptor<SearchCaptureContext> interceptor;
    private ActionListener<SearchResponse> listener;
    private Consumer<ActionListener<SearchResponse>> action;
    private SearchResponse delivered;
    private SearchCaptureContext captured;
    private String query;
    private volatile boolean enabled;

    /** Builds reusable search fixtures and checks that each scenario actually exercises its intended path. */
    @Setup
    public void setup() {
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
        enabled = scenario.equals("disabled") == false;
        interceptor = new QueryInterceptor<>(List.of(new QuerySink<SearchCaptureContext>(() -> "benchmark") {
            @Override
            public boolean isEnabled() {
                return enabled;
            }

            @Override
            public void accept(SearchCaptureContext context) {
                captured = context;
                if (scenario.equals("serialize")) {
                    query = context.getQuery();
                }
            }
        }));
        listener = ActionListener.wrap(r -> delivered = r, e -> { throw new AssertionError(e); });
        action = l -> l.onResponse(response);
    }

    private void invoke() {
        if (scenario.equals("direct")) {
            action.accept(listener);
        } else {
            interceptor.wrapAndRun(listener, new SearchCaptureContextBuilder(task, REGISTRY, request), action);
        }
    }

    /** Measures the work on the search thread, with captured outputs consumed to prevent dead-code elimination. */
    @Benchmark
    public void capture(Blackhole blackhole) {
        invoke();
        blackhole.consume(delivered);
        blackhole.consume(captured);
        blackhole.consume(query);
    }

    /** Releases the fixture's response reference after all invocations finish. */
    @TearDown
    public void teardown() {
        response.decRef();
    }
}
