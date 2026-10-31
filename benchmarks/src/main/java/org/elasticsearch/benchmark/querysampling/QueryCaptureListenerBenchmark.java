/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.querysampling;

import org.elasticsearch.action.search.SearchLogContext;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.querysampling.QuerySamplingSettings;
import org.elasticsearch.xpack.querysampling.capture.QueryCaptureListener;
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

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

/**
 * What the capture listener of the query sampling framework adds to a search, which is the one part of the framework that
 * runs on the search path. The query log calls {@code start} of the listener when every search starts, and that call is
 * what is measured, not the search.
 * <p>
 * The scenarios are the ones a search can be in:
 * <ul>
 * <li>{@code disabled}: sampling is off, a kNN search only meets the check of the setting;</li>
 * <li>{@code not_a_knn_search}: sampling is on and the search is not eligible;</li>
 * <li>{@code knn_not_captured}: an eligible kNN search that the coin flip leaves alone, which is nearly all of them at
 * the default capture rate;</li>
 * <li>{@code knn_captured}: an eligible kNN search that is captured, so that its query is copied and a callback is made
 * for the end of the search.</li>
 * </ul>
 * The listener is shared and its counters are contended, so run it with several threads too, for example
 * {@code -t 8}, to see how it behaves with many searches at once.
 * <p>
 * Results (average time of one {@code start}, ns; Apple M4 Pro, 14 cores, JDK 27-ea, JMH 1.37, 1 fork, 3 warmup and 5
 * measurement iterations of 1 s, with an idle Elasticsearch node running beside it):
 * <pre>
 * scenario            1 thread          8 threads
 * disabled              0.6 ± 0.05        0.6 ± 0.01
 * not_a_knn_search      1.1 ± 0.03        1.2 ± 0.04
 * knn_not_captured      7.5 ± 0.03        9.6 ± 0.21
 * knn_captured         73.7 ± 6.93      224.3 ± 12.05
 * </pre>
 * A search that is not captured pays a few nanoseconds, against search times in milliseconds, and the query log does not
 * follow it. A captured one copies its query and makes the callback for the end of the search, which costs about 75 ns
 * alone and about 225 ns with eight threads at once; the wrapper and the context that the query log adds for a search it
 * follows are not part of this measurement.
 * <p>
 * The listener reads a clock on every kNN search, to bring its capture rate up to date once in a while. With
 * {@code System.nanoTime()} that read alone made {@code knn_not_captured} cost about 395 ns with eight threads on this
 * machine, so the listener is given a cached clock, like the cached time of the thread pool, which the benchmark mimics.
 */
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class QueryCaptureListenerBenchmark {

    private static final int DIMENSIONS = 768;

    @Param({ "disabled", "not_a_knn_search", "knn_not_captured", "knn_captured" })
    public String scenario;

    // like the cached time of the thread pool, which is what the listener is given: one thread updates it, the searches read it
    private volatile long cachedMillis;
    private Thread clockThread;

    private QueryCaptureListener listener;
    private SearchRequest request;
    private Task task;

    @Setup
    public void setUp() {
        BenchmarkLogging.configure(); // the listener logs, so it needs a logger factory
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
        listener = new QueryCaptureListener(
            new ClusterSettings(
                settings,
                Set.of(QuerySamplingSettings.ENABLED, QuerySamplingSettings.CAPTURE_RATE, QuerySamplingSettings.MIN_CAPTURES_PER_HOUR)
            ),
            captured -> {},
            () -> cachedMillis,
            ThreadLocalRandom::current // Randomness.get() fails in this JVM, which has the test framework but no test
        );

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
        task = new Task(1, "transport", TransportSearchAction.NAME, "", TaskId.EMPTY_TASK_ID, Map.of());
    }

    @TearDown
    public void tearDown() {
        clockThread.interrupt();
    }

    @Benchmark
    public Consumer<SearchLogContext> start() {
        return listener.start(task, request);
    }
}
