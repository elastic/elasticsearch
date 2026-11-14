/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.querysampling;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchCaptureContext;
import org.elasticsearch.action.search.SearchCaptureContextBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.querysampling.QuerySamplingSettings;
import org.elasticsearch.xpack.querysampling.capture.QueryCaptureSink;
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
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * What the capture sink of the query sampling framework adds to a search, which is the one part of the framework that runs
 * on the search path. The query interceptor asks the sink whether it is enabled and, if so, hands it the context of every
 * completed search; that is what is measured, not the search, and not the wrapper and the context that the interceptor builds
 * around a search while any sink is enabled.
 * <p>
 * The scenarios are the ones a search can be in:
 * <ul>
 * <li>{@code disabled}: sampling is off, so the sink only answers that it is not enabled;</li>
 * <li>{@code not_a_knn_search}: sampling is on and the search is not eligible;</li>
 * <li>{@code knn_not_captured}: an eligible kNN search that the coin flip leaves alone, which is nearly all of them at
 * the default capture rate;</li>
 * <li>{@code knn_captured}: an eligible kNN search that is captured, so that its query and the ten hits of its response are
 * copied and handed on.</li>
 * </ul>
 * The sink is shared and its counters are contended, so run it with several threads too, for example
 * {@code -t 8}, to see how it behaves with many searches at once.
 * <p>
 * Results (average time of one {@code isEnabled} and {@code accept}, ns; Apple M4 Pro, 14 cores, JDK 27-ea, JMH 1.37, 1 fork,
 * 3 warmup and 5 measurement iterations of 1 s, with an idle Elasticsearch node running beside it):
 * <pre>
 * scenario            1 thread          8 threads
 * disabled              0.5 ± 0.02        0.6 ± 0.00
 * not_a_knn_search      1.4 ± 0.08        1.4 ± 0.02
 * knn_not_captured      7.4 ± 0.09        9.6 ± 0.44
 * knn_captured        118.8 ± 1.80      250.5 ± 10.73
 * </pre>
 * A search that is not captured pays a few nanoseconds, against search times in milliseconds. A captured one copies its query
 * and the hits of its response and hands them on, which costs about 120 ns alone and about 250 ns with eight threads at once.
 * <p>
 * The sink reads a clock on every kNN search, to bring its capture rate up to date once in a while. With
 * {@code System.nanoTime()} that read alone made a search that is not captured cost about 395 ns with eight threads on an
 * Apple M4 Pro, so the sink is given a cached clock, like the cached time of the thread pool, which the benchmark mimics.
 */
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class QueryCaptureSinkBenchmark {

    private static final int DIMENSIONS = 768;
    private static final int HITS = 10;

    @Param({ "disabled", "not_a_knn_search", "knn_not_captured", "knn_captured" })
    public String scenario;

    // like the cached time of the thread pool, which is what the sink is given: one thread updates it, the searches read it
    private volatile long cachedMillis;
    private Thread clockThread;

    private QueryCaptureSink sink;
    private SearchResponse response;
    private SearchCaptureContext context;

    @Setup
    public void setUp() {
        BenchmarkLogging.configure(); // the sink logs, so it needs a logger factory
        cachedMillis = System.nanoTime() / 1_000_000;
        clockThread = new Thread(() -> {
            while (Thread.currentThread().isInterrupted() == false) {
                cachedMillis = System.nanoTime() / 1_000_000;
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(200));
            }
        }, "cached-clock");
        clockThread.setDaemon(true);
        clockThread.start();
        boolean enabled = scenario.equals("disabled") == false;
        double captureRate = scenario.equals("knn_captured") ? 1.0 : 0.0;
        Settings settings = Settings.builder()
            .put(QuerySamplingSettings.ENABLED.getKey(), enabled)
            .put(QuerySamplingSettings.CAPTURE_RATE.getKey(), captureRate)
            .build();
        sink = new QueryCaptureSink(
            new ClusterSettings(
                settings,
                Set.of(QuerySamplingSettings.ENABLED, QuerySamplingSettings.CAPTURE_RATE, QuerySamplingSettings.MIN_CAPTURES_PER_HOUR)
            ),
            captured -> {},
            () -> cachedMillis,
            ThreadLocalRandom::current // Randomness.get() fails in this JVM, which has the test framework but no test
        );

        SearchRequest request;
        if (scenario.equals("not_a_knn_search")) {
            request = new SearchRequest("index").source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()));
        } else {
            Random random = new Random(0);
            float[] vector = new float[DIMENSIONS];
            for (int i = 0; i < vector.length; i++) {
                vector[i] = random.nextFloat();
            }
            KnnSearchBuilder knn = new KnnSearchBuilder("vector", vector, 10, 100, null, null, null).addFilterQuery(
                QueryBuilders.termQuery("category", 3)
            );
            request = new SearchRequest("index").source(new SearchSourceBuilder().knnSearch(List.of(knn)));
        }
        response = response();
        Task task = new Task(1, "transport", TransportSearchAction.NAME, "", TaskId.EMPTY_TASK_ID, Map.of());
        context = new SearchCaptureContextBuilder(task, new NamedWriteableRegistry(List.of()), request).build(response);
    }

    private static SearchResponse response() {
        SearchHit[] hits = new SearchHit[HITS];
        SearchShardTarget shard = new SearchShardTarget("node", new ShardId("index", "_na_", 0), null);
        for (int i = 0; i < HITS; i++) {
            hits[i] = SearchHit.unpooled(i, "doc" + i);
            hits[i].score(1f - i / 100f);
            hits[i].shard(shard);
        }
        SearchHits searchHits = new SearchHits(hits, new TotalHits(HITS, TotalHits.Relation.EQUAL_TO), 1f);
        try {
            return new SearchResponse(
                searchHits,
                InternalAggregations.EMPTY,
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
        } finally {
            searchHits.decRef();
        }
    }

    @TearDown
    public void tearDown() {
        clockThread.interrupt();
        response.decRef();
    }

    @Benchmark
    public void accept(Blackhole blackhole) {
        boolean enabled = sink.isEnabled();
        blackhole.consume(enabled);
        if (enabled) {
            sink.accept(context);
        }
    }
}
