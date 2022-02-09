/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.search;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchShard;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class AsyncSearchTaskTests extends ESTestCase {
    private ThreadPool threadPool;
    private boolean throwOnSchedule = false;

    @Before
    public void beforeTest() {
        threadPool = new TestThreadPool(getTestName()) {
            @Override
            public ScheduledCancellable schedule(Runnable command, TimeValue delay, String executor) {
                if (throwOnSchedule) {
                    throw new RuntimeException();
                }
                return super.schedule(command, delay, executor);
            }
        };
    }

    @After
    public void afterTest() {
        threadPool.shutdownNow();
    }

    private AsyncSearchTask createAsyncSearchTask() {
        return new AsyncSearchTask(
            0L,
            "",
            "",
            new TaskId("node1", 0),
            () -> null,
            TimeValue.timeValueHours(1),
            Collections.emptyMap(),
            Collections.emptyMap(),
            new AsyncExecutionId("0", new TaskId("node1", 1)),
            new NoOpClient(threadPool),
            threadPool,
            (t) -> () -> null
        );
    }

    public void testTaskDescription() {
        SearchRequest searchRequest = new SearchRequest("index1", "index2").source(
            new SearchSourceBuilder().query(QueryBuilders.termQuery("field", "value"))
        );
        AsyncSearchTask asyncSearchTask = new AsyncSearchTask(
            0L,
            "",
            "",
            new TaskId("node1", 0),
            searchRequest::buildDescription,
            TimeValue.timeValueHours(1),
            Collections.emptyMap(),
            Collections.emptyMap(),
            new AsyncExecutionId("0", new TaskId("node1", 1)),
            new NoOpClient(threadPool),
            threadPool,
            (t) -> () -> null
        );
        assertEquals("""
            async_search{indices[index1,index2], search_type[QUERY_THEN_FETCH], source\
            [{"query":{"term":{"field":{"value":"value","boost":1.0}}}}]}""", asyncSearchTask.getDescription());
    }

    public void testWaitForInit() throws InterruptedException {
        AsyncSearchTask task = new AsyncSearchTask(
            0L,
            "",
            "",
            new TaskId("node1", 0),
            () -> null,
            TimeValue.timeValueHours(1),
            Collections.emptyMap(),
            Collections.emptyMap(),
            new AsyncExecutionId("0", new TaskId("node1", 1)),
            new NoOpClient(threadPool),
            threadPool,
            (t) -> () -> null
        );
        int numShards = randomIntBetween(0, 10);
        List<SearchShard> shards = new ArrayList<>();
        for (int i = 0; i < numShards; i++) {
            shards.add(new SearchShard(null, new ShardId("0", "0", 1)));
        }
        List<SearchShard> skippedShards = new ArrayList<>();
        int numSkippedShards = randomIntBetween(0, 10);
        for (int i = 0; i < numSkippedShards; i++) {
            skippedShards.add(new SearchShard(null, new ShardId("0", "0", 1)));
        }

        int numThreads = randomIntBetween(1, 10);
        CountDownLatch latch = new CountDownLatch(numThreads);
        for (int i = 0; i < numThreads; i++) {
            Thread thread = new Thread(() -> task.addCompletionListener(new ActionListener<>() {
                @Override
                public void onResponse(AsyncSearchResponse resp) {
                    assertThat(numShards + numSkippedShards, equalTo(resp.getSearchResponse().getTotalShards()));
                    assertThat(numSkippedShards, equalTo(resp.getSearchResponse().getSkippedShards()));
                    assertThat(0, equalTo(resp.getSearchResponse().getFailedShards()));
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    throw new AssertionError(e);

                }
            }, TimeValue.timeValueMillis(1)));
            thread.start();
        }
        assertFalse(latch.await(numThreads * 2, TimeUnit.MILLISECONDS));
        task.getSearchProgressActionListener().onListShards(shards, skippedShards, SearchResponse.Clusters.EMPTY, false);
        latch.await();
    }

    public void testWithFailure() throws InterruptedException {
        AsyncSearchTask task = createAsyncSearchTask();
        int numThreads = randomIntBetween(1, 10);
        CountDownLatch latch = new CountDownLatch(numThreads);
        for (int i = 0; i < numThreads; i++) {
            Thread thread = new Thread(() -> task.addCompletionListener(new ActionListener<>() {
                @Override
                public void onResponse(AsyncSearchResponse resp) {
                    assertNull(resp.getSearchResponse());
                    assertNotNull(resp.getFailure());
                    assertTrue(resp.isPartial());
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    throw new AssertionError(e);
                }
            }, TimeValue.timeValueMillis(1)));
            thread.start();
        }
        assertFalse(latch.await(numThreads * 2, TimeUnit.MILLISECONDS));
        task.getSearchProgressActionListener().onFailure(new Exception("boom"));
        latch.await();
    }

    public void testWithFailureAndGetResponseFailureDuringReduction() throws InterruptedException {
        AsyncSearchTask task = createAsyncSearchTask();
        task.getSearchProgressActionListener()
            .onListShards(Collections.emptyList(), Collections.emptyList(), SearchResponse.Clusters.EMPTY, false);
        InternalAggregations aggs = InternalAggregations.from(
            Collections.singletonList(
                new StringTerms(
                    "name",
                    BucketOrder.key(true),
                    BucketOrder.key(true),
                    1,
                    1,
                    Collections.emptyMap(),
                    DocValueFormat.RAW,
                    1,
                    false,
                    1,
                    Collections.emptyList(),
                    0L
                )
            )
        );
        task.getSearchProgressActionListener()
            .onPartialReduce(Collections.emptyList(), new TotalHits(0, TotalHits.Relation.EQUAL_TO), aggs, 1);
        task.getSearchProgressActionListener().onFailure(new CircuitBreakingException("boom", CircuitBreaker.Durability.TRANSIENT));
        AtomicReference<AsyncSearchResponse> response = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        task.addCompletionListener(new ActionListener<>() {
            @Override
            public void onResponse(AsyncSearchResponse asyncSearchResponse) {
                assertTrue(response.compareAndSet(null, asyncSearchResponse));
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("onFailure should not be called");
            }
        }, TimeValue.timeValueMillis(10L));
        assertTrue(latch.await(1, TimeUnit.SECONDS));
        AsyncSearchResponse asyncSearchResponse = response.get();
        assertNotNull(response.get().getSearchResponse());
        assertEquals(0, response.get().getSearchResponse().getTotalShards());
        assertEquals(0, response.get().getSearchResponse().getSuccessfulShards());
        assertEquals(0, response.get().getSearchResponse().getFailedShards());
        Exception failure = asyncSearchResponse.getFailure();
        assertThat(failure, instanceOf(ElasticsearchException.class));
        assertEquals("Async search: error while reducing partial results", failure.getMessage());
        assertEquals(1, failure.getSuppressed().length);
        assertThat(failure.getSuppressed()[0], instanceOf(ElasticsearchException.class));
        assertEquals("error while executing search", failure.getSuppressed()[0].getMessage());
        assertThat(failure.getSuppressed()[0].getCause(), instanceOf(CircuitBreakingException.class));
        assertEquals("boom", failure.getSuppressed()[0].getCause().getMessage());
    }

    public void testWaitForCompletion() throws InterruptedException {
        AsyncSearchTask task = createAsyncSearchTask();
        int numShards = randomIntBetween(0, 10);
        List<SearchShard> shards = new ArrayList<>();
        for (int i = 0; i < numShards; i++) {
            shards.add(new SearchShard(null, new ShardId("0", "0", 1)));
        }
        List<SearchShard> skippedShards = new ArrayList<>();
        int numSkippedShards = randomIntBetween(0, 10);
        for (int i = 0; i < numSkippedShards; i++) {
            skippedShards.add(new SearchShard(null, new ShardId("0", "0", 1)));
        }
        int totalShards = numShards + numSkippedShards;
        task.getSearchProgressActionListener().onListShards(shards, skippedShards, SearchResponse.Clusters.EMPTY, false);
        for (int i = 0; i < numShards; i++) {
            task.getSearchProgressActionListener()
                .onPartialReduce(shards.subList(i, i + 1), new TotalHits(0, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO), null, 0);
            assertCompletionListeners(task, totalShards, 1 + numSkippedShards, numSkippedShards, 0, true, false);
        }
        task.getSearchProgressActionListener()
            .onFinalReduce(shards, new TotalHits(0, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO), null, 0);
        assertCompletionListeners(task, totalShards, totalShards, numSkippedShards, 0, true, false);
        ((AsyncSearchTask.Listener) task.getProgressListener()).onResponse(newSearchResponse(totalShards, totalShards, numSkippedShards));
        assertCompletionListeners(task, totalShards, totalShards, numSkippedShards, 0, false, false);
    }

    public void testWithFetchFailures() throws InterruptedException {
        AsyncSearchTask task = createAsyncSearchTask();
        int numShards = randomIntBetween(2, 10);
        List<SearchShard> shards = new ArrayList<>();
        for (int i = 0; i < numShards; i++) {
            shards.add(new SearchShard(null, new ShardId("0", "0", 1)));
        }
        List<SearchShard> skippedShards = new ArrayList<>();
        int numSkippedShards = randomIntBetween(0, 10);
        for (int i = 0; i < numSkippedShards; i++) {
            skippedShards.add(new SearchShard(null, new ShardId("0", "0", 1)));
        }
        int totalShards = numShards + numSkippedShards;
        task.getSearchProgressActionListener().onListShards(shards, skippedShards, SearchResponse.Clusters.EMPTY, false);
        for (int i = 0; i < numShards; i++) {
            task.getSearchProgressActionListener()
                .onPartialReduce(shards.subList(i, i + 1), new TotalHits(0, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO), null, 0);
            assertCompletionListeners(task, totalShards, 1 + numSkippedShards, numSkippedShards, 0, true, false);
        }
        task.getSearchProgressActionListener()
            .onFinalReduce(shards, new TotalHits(0, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO), null, 0);
        int numFetchFailures = randomIntBetween(1, numShards - 1);
        ShardSearchFailure[] shardSearchFailures = new ShardSearchFailure[numFetchFailures];
        for (int i = 0; i < numFetchFailures; i++) {
            IOException failure = new IOException("boum");
            // fetch failures are currently ignored, they come back with onFailure or onResponse anyways
            task.getSearchProgressActionListener().onFetchFailure(i, new SearchShardTarget("0", new ShardId("0", "0", 1), null), failure);
            shardSearchFailures[i] = new ShardSearchFailure(failure);
        }
        assertCompletionListeners(task, totalShards, totalShards, numSkippedShards, 0, true, false);
        ((AsyncSearchTask.Listener) task.getProgressListener()).onResponse(
            newSearchResponse(totalShards, totalShards - numFetchFailures, numSkippedShards, shardSearchFailures)
        );
        assertCompletionListeners(task, totalShards, totalShards - numFetchFailures, numSkippedShards, numFetchFailures, false, false);
    }

    public void testFatalFailureDuringFetch() throws InterruptedException {
        AsyncSearchTask task = createAsyncSearchTask();
        int numShards = randomIntBetween(0, 10);
        List<SearchShard> shards = new ArrayList<>();
        for (int i = 0; i < numShards; i++) {
            shards.add(new SearchShard(null, new ShardId("0", "0", 1)));
        }
        List<SearchShard> skippedShards = new ArrayList<>();
        int numSkippedShards = randomIntBetween(0, 10);
        for (int i = 0; i < numSkippedShards; i++) {
            skippedShards.add(new SearchShard(null, new ShardId("0", "0", 1)));
        }
        int totalShards = numShards + numSkippedShards;
        task.getSearchProgressActionListener().onListShards(shards, skippedShards, SearchResponse.Clusters.EMPTY, false);
        for (int i = 0; i < numShards; i++) {
            task.getSearchProgressActionListener()
                .onPartialReduce(shards.subList(0, i + 1), new TotalHits(0, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO), null, 0);
            assertCompletionListeners(task, totalShards, i + 1 + numSkippedShards, numSkippedShards, 0, true, false);
        }
        task.getSearchProgressActionListener()
            .onFinalReduce(shards, new TotalHits(0, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO), null, 0);
        for (int i = 0; i < numShards; i++) {
            // fetch failures are currently ignored, they come back with onFailure or onResponse anyways
            task.getSearchProgressActionListener()
                .onFetchFailure(i, new SearchShardTarget("0", new ShardId("0", "0", 1), null), new IOException("boum"));
        }
        assertCompletionListeners(task, totalShards, totalShards, numSkippedShards, 0, true, false);
        ((AsyncSearchTask.Listener) task.getProgressListener()).onFailure(new IOException("boum"));
        assertCompletionListeners(task, totalShards, totalShards, numSkippedShards, 0, true, true);
    }

    public void testFatalFailureWithNoCause() throws InterruptedException {
        AsyncSearchTask task = createAsyncSearchTask();
        AsyncSearchTask.Listener listener = task.getSearchProgressActionListener();
        int numShards = randomIntBetween(0, 10);
        List<SearchShard> shards = new ArrayList<>();
        for (int i = 0; i < numShards; i++) {
            shards.add(new SearchShard(null, new ShardId("0", "0", 1)));
        }
        List<SearchShard> skippedShards = new ArrayList<>();
        int numSkippedShards = randomIntBetween(0, 10);
        for (int i = 0; i < numSkippedShards; i++) {
            skippedShards.add(new SearchShard(null, new ShardId("0", "0", 1)));
        }
        int totalShards = numShards + numSkippedShards;
        task.getSearchProgressActionListener().onListShards(shards, skippedShards, SearchResponse.Clusters.EMPTY, false);

        listener.onFailure(new SearchPhaseExecutionException("fetch", "boum", ShardSearchFailure.EMPTY_ARRAY));
        assertCompletionListeners(task, totalShards, 0, numSkippedShards, 0, true, true);
    }

    public void testAddCompletionListenerScheduleErrorWaitForInitListener() throws InterruptedException {
        throwOnSchedule = true;
        AsyncSearchTask asyncSearchTask = createAsyncSearchTask();
        AtomicReference<Exception> failure = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        // onListShards has not been executed, then addCompletionListener has to wait for the
        // onListShards call and is executed as init listener
        asyncSearchTask.addCompletionListener(new ActionListener<>() {
            @Override
            public void onResponse(AsyncSearchResponse asyncSearchResponse) {
                throw new AssertionError("onResponse should not be called");
            }

            @Override
            public void onFailure(Exception e) {
                assertTrue(failure.compareAndSet(null, e));
                latch.countDown();
            }
        }, TimeValue.timeValueMillis(500L));
        asyncSearchTask.getSearchProgressActionListener()
            .onListShards(Collections.emptyList(), Collections.emptyList(), SearchResponse.Clusters.EMPTY, false);
        assertTrue(latch.await(1000, TimeUnit.SECONDS));
        assertThat(failure.get(), instanceOf(RuntimeException.class));
    }

    public void testAddCompletionListenerScheduleErrorInitListenerExecutedImmediately() throws InterruptedException {
        throwOnSchedule = true;
        AsyncSearchTask asyncSearchTask = createAsyncSearchTask();
        asyncSearchTask.getSearchProgressActionListener()
            .onListShards(Collections.emptyList(), Collections.emptyList(), SearchResponse.Clusters.EMPTY, false);
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> failure = new AtomicReference<>();
        // onListShards has already been executed, then addCompletionListener is executed immediately
        asyncSearchTask.addCompletionListener(new ActionListener<>() {
            @Override
            public void onResponse(AsyncSearchResponse asyncSearchResponse) {
                throw new AssertionError("onResponse should not be called");
            }

            @Override
            public void onFailure(Exception e) {
                assertTrue(failure.compareAndSet(null, e));
                latch.countDown();
            }
        }, TimeValue.timeValueMillis(500L));
        assertTrue(latch.await(1000, TimeUnit.SECONDS));
        assertThat(failure.get(), instanceOf(RuntimeException.class));
    }

    private static SearchResponse newSearchResponse(
        int totalShards,
        int successfulShards,
        int skippedShards,
        ShardSearchFailure... failures
    ) {
        InternalSearchResponse response = new InternalSearchResponse(
            SearchHits.EMPTY_WITH_TOTAL_HITS,
            InternalAggregations.EMPTY,
            null,
            null,
            false,
            null,
            1
        );
        return new SearchResponse(
            response,
            null,
            totalShards,
            successfulShards,
            skippedShards,
            100,
            failures,
            SearchResponse.Clusters.EMPTY
        );
    }

    private static void assertCompletionListeners(
        AsyncSearchTask task,
        int expectedTotalShards,
        int expectedSuccessfulShards,
        int expectedSkippedShards,
        int expectedShardFailures,
        boolean isPartial,
        boolean totalFailureExpected
    ) throws InterruptedException {
        int numThreads = randomIntBetween(1, 10);
        CountDownLatch latch = new CountDownLatch(numThreads);
        for (int i = 0; i < numThreads; i++) {
            Thread thread = new Thread(() -> task.addCompletionListener(new ActionListener<>() {
                @Override
                public void onResponse(AsyncSearchResponse resp) {
                    assertThat(resp.getSearchResponse().getTotalShards(), equalTo(expectedTotalShards));
                    assertThat(resp.getSearchResponse().getSuccessfulShards(), equalTo(expectedSuccessfulShards));
                    assertThat(resp.getSearchResponse().getSkippedShards(), equalTo(expectedSkippedShards));
                    assertThat(resp.getSearchResponse().getFailedShards(), equalTo(expectedShardFailures));
                    assertThat(resp.isPartial(), equalTo(isPartial));
                    if (expectedShardFailures > 0) {
                        assertThat(resp.getSearchResponse().getShardFailures().length, equalTo(expectedShardFailures));
                        for (ShardSearchFailure failure : resp.getSearchResponse().getShardFailures()) {
                            assertThat(failure.getCause(), instanceOf(IOException.class));
                            assertThat(failure.getCause().getMessage(), equalTo("boum"));
                        }
                    }
                    if (totalFailureExpected) {
                        assertNotNull(resp.getFailure());
                    } else {
                        assertNull(resp.getFailure());
                    }
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    throw new AssertionError(e);
                }
            }, TimeValue.timeValueMillis(1)));
            thread.start();
        }
        latch.await();
    }
}
