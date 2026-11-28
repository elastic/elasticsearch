/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.heap_attack;

import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.util.EntityUtils;
import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.extractValue;

/**
 * Manual benchmark, run with {@code -Dtests.heap_attack.fillnull.parallel=true}: measures the memory footprint of several
 * {@code FROM idx | FILLNULL 0 ON *} queries running <b>concurrently</b>, to answer elastic/esql-planning#2419 - whether the
 * untracked per-column cost of {@code FILLNULL ON *} (the {@code Page} block-array copies no breaker accounts for) can stack
 * across parallel queries and push a node over the edge at the proposed 1000-column cap.
 * <p>
 * For each concurrency K it runs a sustained burst: K threads issue the query back-to-back for a fixed window while a sampler
 * records the peak request-breaker size and peak heap across all nodes. The breaker peak is the memory the engine tracks; the
 * gap {@code heap_peak - breaker_peak} is a proxy for everything it does not, including the block-array copies. Comparing that
 * gap for {@code fillnull} against the {@code baseline} (same query without {@code FILLNULL}) isolates FILLNULL's untracked
 * footprint, since the JVM's steady-state heap and the tracked block data are common to both.
 * <p>
 * A sustained burst is used rather than one shot because a single 1000-column query finishes in ~100ms, far quicker than any
 * node-stats sampler can reliably catch its peak; keeping K queries in flight for a few seconds gives a stable peak and models
 * the sustained-concurrency scenario the issue is concerned with. Each burst is kept well under 5 minutes to stay clear of the
 * suite's out-of-memory backstop.
 */
@TimeoutSuite(millis = 180 * TimeUnits.MINUTE)
public class HeapAttackFillNullParallelIT extends HeapAttackFillNullTestCase {
    private static final String PARALLEL = "tests.heap_attack.fillnull.parallel";
    private static final String PARALLEL_FIELDS = PARALLEL + ".fields";
    private static final String PARALLEL_DOCS = PARALLEL + ".docs";
    private static final String PARALLEL_CONCURRENCY = PARALLEL + ".concurrency";
    private static final String PARALLEL_SHAPES = PARALLEL + ".shapes";
    private static final String PARALLEL_DURATION_SECONDS = PARALLEL + ".duration_seconds";

    private static final String DEFAULT_SHAPES = "MIXED_NULL,ALL_NULL";

    public void testParallelFillNull() throws Exception {
        assumeTrue(
            "set -D" + PARALLEL + "=true to run the FILLNULL parallel benchmark",
            Booleans.parseBoolean(System.getProperty(PARALLEL, "false"))
        );
        int fields = Integer.parseInt(System.getProperty(PARALLEL_FIELDS, "1000"));
        int docs = Integer.parseInt(System.getProperty(PARALLEL_DOCS, "1000"));
        long durationMillis = Long.parseLong(System.getProperty(PARALLEL_DURATION_SECONDS, "30")) * 1000;
        List<Integer> concurrencies = Arrays.stream(System.getProperty(PARALLEL_CONCURRENCY, "1,2,4,8").split(","))
            .map(s -> Integer.parseInt(s.trim()))
            .toList();
        List<Shape> shapes = Arrays.stream(System.getProperty(PARALLEL_SHAPES, DEFAULT_SHAPES).split(","))
            .map(s -> Shape.valueOf(s.trim()))
            .toList();

        allowFillNullOnAll(fields);
        for (Shape shape : shapes) {
            initWideIndex(fields, docs, shape);
            try {
                for (int concurrency : concurrencies) {
                    // baseline first so its peak does not include memory the fillnull run has not yet released
                    burst(shape, fields, docs, concurrency, durationMillis, "baseline", wideQuery(false, docs, false));
                    burst(shape, fields, docs, concurrency, durationMillis, "fillnull", wideQuery(true, docs, false));
                }
            } finally {
                deleteIndex(INDEX);
            }
        }
    }

    /**
     * Runs {@code concurrency} threads issuing {@code body} back-to-back for {@code durationMillis}, under a node sampler, and
     * logs one {@code FILLNULL_PARALLEL} line with the peak heap and breaker observed during the window.
     */
    private void burst(Shape shape, int fields, int docs, int concurrency, long durationMillis, String variant, String body)
        throws Exception {
        CountDownLatch ready = new CountDownLatch(concurrency);
        CountDownLatch start = new CountDownLatch(1);
        AtomicLong completed = new AtomicLong();
        AtomicLong failed = new AtomicLong();
        List<Thread> threads = new ArrayList<>(concurrency);
        long deadline = System.nanoTime() + durationMillis * 1_000_000;

        for (int i = 0; i < concurrency; i++) {
            Thread thread = new Thread(() -> {
                ready.countDown();
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                while (System.nanoTime() < deadline) {
                    try {
                        performQuery(body);
                        completed.incrementAndGet();
                    } catch (ResponseException e) {
                        // a tripped breaker is an expected outcome under load, not a harness error - count it and keep going
                        failed.incrementAndGet();
                    } catch (IOException e) {
                        failed.incrementAndGet();
                        logger.warn("parallel [{}] query io error", variant, e);
                    }
                }
            }, "fillnull-parallel-" + variant + "-" + i);
            threads.add(thread);
            thread.start();
        }

        Map<String, Object> metrics = new LinkedHashMap<>();
        try (Sampler sampler = new Sampler()) {
            ready.await();
            long wallStart = System.nanoTime();
            start.countDown();
            for (Thread thread : threads) {
                thread.join();
            }
            long wallMillis = (System.nanoTime() - wallStart) / 1_000_000;
            sampler.stop();
            long heapPeakMb = sampler.maxHeapBytes / (1024 * 1024);
            long breakerPeakMb = sampler.maxBreakerBytes / (1024 * 1024);
            metrics.put("wall_ms", wallMillis);
            metrics.put("completed", completed.get());
            metrics.put("failed", failed.get());
            metrics.put("heap_peak_mb", heapPeakMb);
            metrics.put("breaker_peak_mb", breakerPeakMb);
            metrics.put("heap_minus_breaker_mb", heapPeakMb - breakerPeakMb);
            metrics.put("gc_ms", sampler.gcMillisAfter - sampler.gcMillisBefore);
            if (sampler.failure != null) {
                metrics.put("sampler_error", sampler.failure.getClass().getSimpleName());
            }
        }
        StringJoiner line = new StringJoiner(" ");
        line.add("shape=" + shape).add("fields=" + fields).add("docs=" + docs).add("concurrency=" + concurrency).add("variant=" + variant);
        metrics.forEach((k, v) -> line.add(k + "=" + v));
        logger.info("FILLNULL_PARALLEL {}", line);
    }

    /**
     * Issues one {@code _query} with the raw client, draining the response. Deliberately does not go through
     * {@link #query}, whose per-call {@code TestThreadPool} out-of-memory backstop would add heap churn to a tight loop and
     * blur the measurement; a sustained burst stays far under the 5-minute backstop window anyway.
     */
    private void performQuery(String body) throws IOException {
        Request request = new Request("POST", "/_query");
        request.addParameter("error_trace", "");
        request.addParameter("filter_path", "took");
        request.setJsonEntity(body.replace("\n", "\\n"));
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder()
                .setRequestConfig(RequestConfig.custom().setSocketTimeout(Math.toIntExact(TimeValue.timeValueMinutes(1).millis())).build())
                .setWarningsHandler(WarningsHandler.PERMISSIVE)
        );
        Response response = client().performRequest(request);
        EntityUtils.consumeQuietly(response.getEntity());
    }

    private static long number(Object value) {
        return value instanceof Number n ? n.longValue() : 0;
    }

    /** Samples request-breaker and heap usage on every node while a burst runs; breakers expose no peak of their own. */
    private final class Sampler implements AutoCloseable {
        private static final String STATS = "/_nodes/stats/jvm,breaker";
        private static final String STATS_FILTER =
            "nodes.*.jvm.mem.heap_used_in_bytes,nodes.*.jvm.gc.collectors.*.collection_time_in_millis,"
                + "nodes.*.breakers.request.estimated_size_in_bytes";

        private final Thread thread;
        private volatile boolean stopped;
        private volatile long maxHeapBytes;
        private volatile long maxBreakerBytes;
        private volatile Exception failure;
        private final long gcMillisBefore;
        private long gcMillisAfter;

        Sampler() throws IOException {
            gcMillisBefore = sample();
            thread = new Thread(() -> {
                while (stopped == false) {
                    try {
                        sample();
                        Thread.sleep(100);
                    } catch (IOException | RuntimeException | InterruptedException e) {
                        failure = e;
                        return;
                    }
                }
            }, "fillnull-parallel-sampler");
            thread.setDaemon(true);
            thread.start();
        }

        /** Waits for the sampling thread so the final sample does not race it, then takes that sample. */
        void stop() throws IOException {
            close();
            gcMillisAfter = sample();
        }

        /** Records one sample and returns the total GC time across all nodes and collectors. */
        @SuppressWarnings("unchecked")
        private synchronized long sample() throws IOException {
            Request request = new Request("GET", STATS);
            request.addParameter("filter_path", STATS_FILTER);
            Map<String, Object> nodes = (Map<String, Object>) responseAsMap(adminClient().performRequest(request)).get("nodes");
            long gcMillis = 0;
            for (Object node : nodes.values()) {
                Map<?, ?> stats = (Map<?, ?>) node;
                maxHeapBytes = Math.max(maxHeapBytes, number(extractValue(stats, "jvm", "mem", "heap_used_in_bytes")));
                maxBreakerBytes = Math.max(maxBreakerBytes, number(extractValue(stats, "breakers", "request", "estimated_size_in_bytes")));
                if (extractValue(stats, "jvm", "gc", "collectors") instanceof Map<?, ?> collectors) {
                    for (Object collector : collectors.values()) {
                        gcMillis += number(((Map<?, ?>) collector).get("collection_time_in_millis"));
                    }
                }
            }
            return gcMillis;
        }

        @Override
        public void close() {
            stopped = true;
            try {
                thread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
