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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

@SuiteScopeTestCase
public class AsyncSearchConcurrentStatusIT extends AsyncSearchIntegTestCase {
    private static String indexName;
    private static int numShards;

    private static int numKeywords;

    @Override
    public void setupSuiteScopeCluster() {
        indexName = "test-async";
        numShards = randomIntBetween(1, 20);
        int numDocs = randomIntBetween(100, 1000);
        createIndex(indexName, Settings.builder().put("index.number_of_shards", numShards).build());
        numKeywords = randomIntBetween(50, 100);
        Set<String> keywordSet = new HashSet<>();
        for (int i = 0; i < numKeywords; i++) {
            keywordSet.add(randomAlphaOfLengthBetween(10, 20));
        }
        numKeywords = keywordSet.size();
        String[] keywords = keywordSet.toArray(String[]::new);
        List<IndexRequestBuilder> reqs = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            float metric = randomFloat();
            String keyword = keywords[randomIntBetween(0, numKeywords - 1)];
            reqs.add(prepareIndex(indexName).setSource("terms", keyword, "metric", metric));
        }
        indexRandom(true, true, reqs);
    }

    /**
     * The test spins up a set of poller threads that repeatedly call {@code _async_search/{d]}}
     * but hold them behind a latch. Once all pollers are ready, the latch is released so they
     * begin hammering the async search endpoint at the same time. In parallel, a consumer thread
     * drives the async search to completion using the blocking iterator. This coordinated overlap
     * ensures we exercise the window where the task is closing and some status calls may return
     * {@code 410 GONE}.
     */
    public void testConcurrentStatusFetchWhileTaskCloses() throws Exception {
        final String aggName = "terms";
        final SearchSourceBuilder source = new SearchSourceBuilder().aggregation(
            AggregationBuilders.terms(aggName).field("terms.keyword").size(Math.max(1, numKeywords))
        );

        final int progressStep = (numShards > 2) ? randomIntBetween(2, numShards) : 2;
        try (SearchResponseIterator it = assertBlockingIterator(indexName, numShards, source, 0, progressStep)) {
            String id = getAsyncId(it);

            PollStats stats = new PollStats();

            // Pick a random number of status-poller threads, at least 1, up to (4×numShards)
            int pollerThreads = randomIntBetween(1, 4 * numShards);
            PollerGroup pollers = startGetStatusThreadsHot(id, pollerThreads, aggName, stats);
            pollers.start(); // release pollers

            // Finish consumption on a separate thread and capture errors
            var consumerExec = Executors.newSingleThreadExecutor();
            AtomicReference<Throwable> consumerError = new AtomicReference<>();
            Future<?> consumer = consumerExec.submit(() -> {
                try {
                    consumeAllResponses(it, aggName);
                } catch (Throwable t) {
                    consumerError.set(t);
                }
            });

            Thread.sleep(randomIntBetween(100, 200));

            TimeValue timeout = TimeValue.timeValueSeconds(3);

            // Join consumer & surface errors
            try {
                consumer.get(timeout.millis(), TimeUnit.MILLISECONDS);

                if (consumerError.get() != null) {
                    fail("consumeAllResponses failed: " + consumerError.get());
                }
            } catch (TimeoutException e) {
                consumer.cancel(true);
                fail(e, "Consumer thread did not finish within timeout");
            } catch (Exception ignored) {} finally {

                try {
                    pollers.stopAndAwait(timeout);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    fail("Interrupted while stopping pollers: " + e.getMessage());
                }

                consumerExec.shutdown();
                try {
                    consumerExec.awaitTermination(timeout.millis(), TimeUnit.MILLISECONDS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }

            assertNoWorkerFailures(pollers);
            assertStats(stats);
        }
    }

    private void assertNoWorkerFailures(PollerGroup pollers) {
        List<Throwable> failures = pollers.getFailures();
        assertTrue(
            "Unexpected worker failures:\n" + failures.stream().map(ExceptionsHelper::stackTrace).reduce("", (a, b) -> a + "\n---\n" + b),
            failures.isEmpty()
        );
    }

    private void assertStats(PollStats stats) {
        assertEquals(stats.totalCalls.sum(), stats.runningResponses.sum() + stats.completedResponses.sum());
        assertEquals("There should be no exceptions other than GONE", 0, stats.exceptions.sum());
    }

    private String getAsyncId(SearchResponseIterator it) {
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

    private PollerGroup startGetStatusThreadsHot(String id, int threads, String aggName, PollStats stats) {
        final ExecutorService exec = Executors.newFixedThreadPool(threads);
        final List<Future<?>> futures = new ArrayList<>(threads);
        final AtomicBoolean running = new AtomicBoolean(true);
        final CountDownLatch startGate = new CountDownLatch(1);

        for (int i = 0; i < threads; i++) {
            futures.add(exec.submit(() -> {
                startGate.await();
                while (running.get()) {
                    AsyncSearchResponse resp = null;
                    try {
                        resp = getAsyncSearch(id);
                        stats.totalCalls.increment();

                        if (resp.isRunning()) {
                            stats.runningResponses.increment();
                        } else {
                            // Success-only assertions: if reported completed, we must have a proper search response
                            assertNull("Async search reported completed with failure", resp.getFailure());
                            assertNotNull("Completed async search must carry a SearchResponse", resp.getSearchResponse());
                            assertNotNull("Completed async search must have aggregations", resp.getSearchResponse().getAggregations());
                            assertNotNull(
                                "Completed async search must contain the expected aggregation",
                                resp.getSearchResponse().getAggregations().get(aggName)
                            );
                            stats.completedResponses.increment();
                        }
                    } catch (Exception e) {
                        Throwable cause = ExceptionsHelper.unwrapCause(e);
                        if (cause instanceof ElasticsearchStatusException) {
                            RestStatus status = ExceptionsHelper.status(cause);
                            if (status == RestStatus.GONE) {
                                stats.gone410.increment();
                            }
                        } else {
                            stats.exceptions.increment();
                        }
                    } finally {
                        if (resp != null) {
                            resp.decRef();
                        }
                    }
                }
                return null;
            }));
        }
        return new PollerGroup(exec, futures, running, startGate);
    }

    static final class PollStats {
        final LongAdder totalCalls = new LongAdder();
        final LongAdder runningResponses = new LongAdder();
        final LongAdder completedResponses = new LongAdder();
        final LongAdder exceptions = new LongAdder();
        final LongAdder gone410 = new LongAdder();
    }

    static class PollerGroup {
        private final ExecutorService exec;
        private final List<Future<?>> futures;
        // The threads are created right away, but they immediately block on the latch
        private final AtomicBoolean running;
        private final CountDownLatch startGate;

        private PollerGroup(ExecutorService exec, List<Future<?>> futures, AtomicBoolean running, CountDownLatch startGate) {
            this.exec = exec;
            this.futures = futures;
            this.running = running;
            this.startGate = startGate;
        }

        void start() { startGate.countDown(); }

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
                } catch (CancellationException ignored) {} catch (ExecutionException ee) {
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
