/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.search;

import com.carrotsearch.randomizedtesting.annotations.Repeat;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase.SuiteScopeTestCase;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

@SuiteScopeTestCase
public class AsyncSearchGoneIT extends AsyncSearchIntegTestCase {
    private static String indexName;
    private static int numShards;

    private static int numKeywords;
    private static Map<String, AtomicInteger> keywordFreqs;
    private static float maxMetric = Float.NEGATIVE_INFINITY;
    private static float minMetric = Float.POSITIVE_INFINITY;

    @Override
    public void setupSuiteScopeCluster() throws InterruptedException {
        indexName = "test-async";
        numShards = randomIntBetween(1, 20);
        int numDocs = randomIntBetween(100, 1000);
        createIndex(indexName, Settings.builder().put("index.number_of_shards", numShards).build());
        numKeywords = randomIntBetween(50, 100);
        keywordFreqs = new HashMap<>();
        Set<String> keywordSet = new HashSet<>();
        for (int i = 0; i < numKeywords; i++) {
            keywordSet.add(randomAlphaOfLengthBetween(10, 20));
        }
        numKeywords = keywordSet.size();
        String[] keywords = keywordSet.toArray(String[]::new);
        List<IndexRequestBuilder> reqs = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            float metric = randomFloat();
            maxMetric = Math.max(metric, maxMetric);
            minMetric = Math.min(metric, minMetric);
            String keyword = keywords[randomIntBetween(0, numKeywords - 1)];
            keywordFreqs.compute(keyword, (k, v) -> {
                if (v == null) {
                    return new AtomicInteger(1);
                }
                v.incrementAndGet();
                return v;
            });
            reqs.add(prepareIndex(indexName).setSource("terms", keyword, "metric", metric));
        }
        indexRandom(true, true, reqs);
    }

    @Repeat(iterations = 10000)
    public void testConcurrentStatusFetchWhileTaskCloses() throws Exception {
        CountDownLatch beforeClose = new CountDownLatch(1);

        final SearchSourceBuilder source = new SearchSourceBuilder()
            .aggregation(AggregationBuilders.terms("terms").field("terms.keyword").size(Math.max(1, numKeywords)));

        final int progressStep = (numShards > 2) ? randomIntBetween(2, numShards) : 2;
        try (SearchResponseIterator it = assertBlockingIterator(indexName, numShards, source, 0, progressStep)) {
            setBeforeCloseLatch(beforeClose);

            String id = getAsyncId(it);

            PollStats stats = new PollStats();
            StartableThreadGroup pollers = startGetStatusThreadsHot(id, 32, stats);
            pollers.startHotThreads.countDown(); // release workers

            //Thread.sleep(100);

            // Finish consumption on a separate thread and capture errors
            var finisherExec = Executors.newSingleThreadExecutor();
            var finisherError = new AtomicReference<Throwable>();
            Future<?> finisher = finisherExec.submit(() -> {
                try {
                    consumeAllResponses(it, "terms");
                } catch (Throwable t) {
                    finisherError.set(t);
                }
            });

            // Align with the exact completion edge
            beforeClose.await();

            // Keep pollers hot long enough to land inside the small GONE window
            pollers.stopAndAwait(TimeValue.timeValueSeconds(3));

            // Join finisher & surface errors
            try {
                finisher.get();
            } catch (Exception ignored) {
            } finally { finisherExec.shutdown(); }

            if (finisherError.get() != null) {
                fail("consumeAllResponses failed: " + finisherError.get());
            }

            printPollerStats(stats);
            assertGoneSeen(stats);
        }
    }

    private void printPollerStats(PollStats stats) {
        System.out.println("Total:" + stats.totalCalls.sum());
        System.out.println("Completed:" + stats.completedResponses.sum());
        System.out.println("Running:" + stats.runningResponses.sum());
        System.out.println("Exceptions:" + stats.exceptions.sum());
        System.out.println("Gone:" + stats.gone410.sum());
    }

    private void assertGoneSeen(PollStats stats) {
        assertFalse("Expected at least one GONE (410) from concurrent STATUS during task close; totals=" + stats.totalCalls.sum(),
            stats.gone410.sum() > 0L);
    }

    private String getAsyncId(SearchResponseIterator it)  {
        AsyncSearchResponse response = it.next();
        try {
            assertNotNull(response.getId());
            return response.getId();
        } finally {
            response.decRef();
        }
    }

    private void consumeAllResponses(SearchResponseIterator it, String aggName) throws Exception {
        while (it.hasNext()) {
            AsyncSearchResponse response = it.next();
            try {
                if (response.getSearchResponse() != null && response.getSearchResponse().getAggregations() != null) {
                    assertNotNull(response.getSearchResponse().getAggregations().get(aggName));
                }
            } finally {
                response.decRef();
            }
        }
    }

    private StartableThreadGroup startGetStatusThreadsHot(String id, int threads, PollStats stats) {
        final ExecutorService exec = Executors.newFixedThreadPool(threads);
        final List<Future<?>> futures = new ArrayList<>(threads);
        final AtomicBoolean running = new AtomicBoolean(true);
        final CountDownLatch start = new CountDownLatch(1);

        for (int i = 0; i < threads; i++) {
            futures.add(exec.submit(() -> {
                start.await();
                while (running.get()) {
                    AsyncSearchResponse resp = null;
                    try {
                        resp = getAsyncSearch(id);
                        stats.totalCalls.increment();

                        if (resp.isRunning()) {
                            stats.runningResponses.increment();
                        } else {
                            stats.completedResponses.increment();
                        }
                    } catch (Exception e) {
                        Throwable cause = ExceptionsHelper.unwrapCause(e);
                        if (cause instanceof ElasticsearchStatusException) {
                            RestStatus st = ExceptionsHelper.status(cause);
                            if (st == RestStatus.GONE) {
                                stats.gone410.increment();
                                continue;
                            }
                            if (st == RestStatus.NOT_FOUND) {
                                continue;
                            }
                        }
                        continue;
                    } finally {
                        if (resp != null) {
                            resp.decRef();
                        }
                    }
                }
                return null;
            }));
        }
        return new StartableThreadGroup(exec, futures, running, start);
    }

    // (Result pollers unchanged/not used for GONE case)

    static final class PollStats {
        final LongAdder totalCalls = new LongAdder();
        final LongAdder runningResponses = new LongAdder();
        final LongAdder completedResponses = new LongAdder();
        final LongAdder hadSearchResponse = new LongAdder(); // not used by STATUS
        final LongAdder exceptions = new LongAdder();
        final LongAdder gone410 = new LongAdder();
    }

    private final class StartableThreadGroup extends ThreadGroup {
        CountDownLatch startHotThreads;
        StartableThreadGroup(ExecutorService exec, List<Future<?>> futures, AtomicBoolean running, CountDownLatch startHotThreads) {
            super(exec, futures, running);
            this.startHotThreads = startHotThreads;
        }
    }

    class ThreadGroup {
        private final ExecutorService exec;
        private final List<Future<?>>  futures;
        private final AtomicBoolean running;

        private ThreadGroup(ExecutorService exec, List<Future<?>> futures, AtomicBoolean running) {
            this.exec = exec;
            this.futures = futures;
            this.running = running;
        }

        void stopAndAwait(TimeValue timeout) throws InterruptedException {
            running.set(false);
            exec.shutdown();
            if (exec.awaitTermination(timeout.millis(), TimeUnit.MILLISECONDS) == false) {
                exec.shutdownNow();
                exec.awaitTermination(timeout.millis(), TimeUnit.MILLISECONDS);
            }
        }

        List<Throwable> getFailures() {
            List<Throwable> failures = new ArrayList<>();
            for (Future<?> f : futures) {
                try {
                    f.get();
                } catch (CancellationException ignored) {
                } catch (ExecutionException ee) {
                    failures.add(ExceptionsHelper.unwrapCause(ee.getCause()));
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    if (failures.isEmpty()) failures.add(ie);
                }
            }
            return failures;
        }
    }
}
