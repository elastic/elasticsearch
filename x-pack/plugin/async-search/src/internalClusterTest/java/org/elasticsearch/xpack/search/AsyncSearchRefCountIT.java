/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.search;

import com.carrotsearch.randomizedtesting.annotations.Repeat;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.DummyQueryBuilder;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.InternalTerms;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.Min;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.query.ThrowingQueryBuilder;
import org.elasticsearch.test.ESIntegTestCase.SuiteScopeTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.search.action.AsyncStatusResponse;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchRequest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.search.SearchService.MAX_ASYNC_SEARCH_RESPONSE_SIZE_SETTING;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

@TestLogging(
    reason = "testing debug log output to identify race condition",
    value = "org.elasticsearch.xpack.search.MutableSearchResponse:DEBUG,org.elasticsearch.xpack.search.AsyncSearchTask:DEBUG"
)
@SuiteScopeTestCase
public class AsyncSearchRefCountIT extends AsyncSearchIntegTestCase {
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

    /**
     * Category 1, point 2 : We receive the final underlying search response, then the AsyncSearchTask#close()
     *                       is invoked when we finish consuming the iterator.
     * Category 2, point 1 : We execute repeatedly GET _async_search/{id}
     */
    //@Repeat(iterations = 1000)
    public void testConcurrentStatusFetchWhileTaskCloses() throws Exception {
        final TimeValue WAIT = TimeValue.timeValueMillis(100);

        final SearchSourceBuilder source = new SearchSourceBuilder()
            .aggregation(AggregationBuilders.terms("terms").field("terms.keyword").size(Math.max(1, numKeywords)));

        final int progressStep = (numShards > 2) ? randomIntBetween(2, numShards) : 2;
        try (SearchResponseIterator it = assertBlockingIterator(indexName, numShards, source, 0, progressStep)) {
            String id = getAsyncId(it);
            ThreadGroup getThreads = startGetResultThreads(id, 10);

            Thread.sleep(WAIT.millis());
            consumeAllResponses(it, "terms");
            Thread.sleep(WAIT.millis());

            getThreads.stopAndAwait(WAIT);
            cleanupAsyncSearch(id);

            assertError(getThreads.getFailures());
        }
    }

    private void assertError( List<Throwable> fails ) {
        if (fails.isEmpty() == false) {
            for (Throwable failure : fails) {
                final String msg = String.valueOf(failure.getMessage());
                if (failure instanceof AssertionError && msg.contains("already closed, can't increment ref count")) {
                    fail("Race detected in poller thread:\n" + ExceptionsHelper.stackTrace(failure));
                } else {
                    fail("Unexpected exception in poller thread:\n" + ExceptionsHelper.stackTrace(failure));
                }
            }
        }
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
                System.out.println("Async response: " + response.getSearchResponse().toString());
            } finally {
                response.decRef();
            }
        }
    }

    private void cleanupAsyncSearch(String id) throws Exception {
        ensureTaskCompletion(id);
        deleteAsyncSearch(id);
        ensureTaskRemoval(id);
    }

    private ThreadGroup startGetResultThreads(String id, int threads) {
        final ExecutorService exec = Executors.newFixedThreadPool(threads);
        final List<Future<?>> futures = new ArrayList<>(threads);
        final AtomicBoolean running = new AtomicBoolean(true);

        for (int i = 0; i < threads; i++) {
            futures.add(exec.submit(() -> {
                while (running.get()) {
                    AsyncSearchResponse resp = null;
                    try {
                        resp = getAsyncSearch(id);
                        System.out.println("Thread" + Thread.currentThread().getName() + " isRunning: " + resp.isRunning());
                        if (resp.isRunning()) {  }
                    } catch (Exception e) {
                        System.out.println(e.getMessage());
                        throw new IllegalArgumentException();
                    } finally {
                        if (resp != null) {
                            resp.decRef();
                        }
                    }
                }
            }));
        }
        return new ThreadGroup(exec, futures, running);
    }


    private final class ThreadGroup {

        private final ExecutorService exec;
        private final List<Future<?>>  futures;
        private final AtomicBoolean running;

        private ThreadGroup(ExecutorService exec, List<Future<?>> futures, AtomicBoolean running) {
            this.exec = exec;
            this.futures = futures;
            this.running = running;
        }

        void stopAndAwait(TimeValue timeoutMillis) throws InterruptedException {
            running.set(false);
            exec.shutdown();
            if (exec.awaitTermination(timeoutMillis.millis(), java.util.concurrent.TimeUnit.MILLISECONDS) == false) {
                exec.shutdownNow();
                exec.awaitTermination(timeoutMillis.millis(), java.util.concurrent.TimeUnit.MILLISECONDS);
            }
        }

        List<Throwable> getFailures() {
            List<Throwable> failures = new ArrayList<>();
            for (Future<?> f : futures) {
                try {
                    f.get();
                } catch (CancellationException ignored) {
                } catch (ExecutionException ee) {
                    failures.add(ee.getCause());
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    if (failures.isEmpty()) failures.add(ie);
                }
            }
            return failures;
        }
    }
}
