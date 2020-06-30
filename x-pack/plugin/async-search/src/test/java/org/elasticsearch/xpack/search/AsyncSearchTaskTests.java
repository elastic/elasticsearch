/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchShard;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.InternalAggregations;
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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class AsyncSearchTaskTests extends ESTestCase {
    private ThreadPool threadPool;

    @Before
    public void beforeTest() {
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void afterTest() {
        threadPool.shutdownNow();
    }

    private AsyncSearchTask createAsyncSearchTask() {
        return new AsyncSearchTask(0L, "", "", new TaskId("node1", 0), TimeValue.timeValueHours(1),
            Collections.emptyMap(), Collections.emptyMap(), new AsyncExecutionId("0", new TaskId("node1", 1)),
            new NoOpClient(threadPool), threadPool, null);
    }

    public void testWaitForInit() throws InterruptedException {
        AsyncSearchTask task = new AsyncSearchTask(0L, "", "", new TaskId("node1", 0), TimeValue.timeValueHours(1),
            Collections.emptyMap(), Collections.emptyMap(), new AsyncExecutionId("0", new TaskId("node1", 1)),
            new NoOpClient(threadPool), threadPool, null);
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
        assertFalse(latch.await(numThreads*2, TimeUnit.MILLISECONDS));
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
        assertFalse(latch.await(numThreads*2, TimeUnit.MILLISECONDS));
        task.getSearchProgressActionListener().onFailure(new Exception("boom"));
        latch.await();
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
            task.getSearchProgressActionListener().onPartialReduce(shards.subList(i, i+1),
                new TotalHits(0, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO), null, 0);
            assertCompletionListeners(task, totalShards, 1 + numSkippedShards, numSkippedShards, 0, true);
        }
        task.getSearchProgressActionListener().onFinalReduce(shards,
            new TotalHits(0, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO), null, 0);
        assertCompletionListeners(task, totalShards, totalShards, numSkippedShards, 0, true);
        ((AsyncSearchTask.Listener)task.getProgressListener()).onResponse(
            newSearchResponse(totalShards, totalShards, numSkippedShards));
        assertCompletionListeners(task, totalShards, totalShards, numSkippedShards, 0, false);
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
            task.getSearchProgressActionListener().onPartialReduce(shards.subList(i, i+1),
                new TotalHits(0, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO), null, 0);
            assertCompletionListeners(task, totalShards, 1 + numSkippedShards, numSkippedShards, 0, true);
        }
        task.getSearchProgressActionListener().onFinalReduce(shards,
            new TotalHits(0, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO), null, 0);
        int numFetchFailures = randomIntBetween(1, numShards - 1);
        ShardSearchFailure[] shardSearchFailures = new ShardSearchFailure[numFetchFailures];
        for (int i = 0; i < numFetchFailures; i++) {
            IOException failure = new IOException("boum");
            task.getSearchProgressActionListener().onFetchFailure(i,
                new SearchShardTarget("0", new ShardId("0", "0", 1), null, OriginalIndices.NONE),
                failure);
            shardSearchFailures[i] = new ShardSearchFailure(failure);
        }
        assertCompletionListeners(task, totalShards, totalShards, numSkippedShards, numFetchFailures, true);
        ((AsyncSearchTask.Listener)task.getProgressListener()).onResponse(
            newSearchResponse(totalShards, totalShards - numFetchFailures, numSkippedShards, shardSearchFailures));
        assertCompletionListeners(task, totalShards, totalShards - numFetchFailures, numSkippedShards, numFetchFailures, false);
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
            task.getSearchProgressActionListener().onPartialReduce(shards.subList(0, i+1),
                new TotalHits(0, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO), null, 0);
            assertCompletionListeners(task, totalShards, i + 1 + numSkippedShards, numSkippedShards, 0, true);
        }
        task.getSearchProgressActionListener().onFinalReduce(shards,
            new TotalHits(0, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO), null, 0);
        for (int i = 0; i < numShards; i++) {
            task.getSearchProgressActionListener().onFetchFailure(i,
                new SearchShardTarget("0", new ShardId("0", "0", 1), null, OriginalIndices.NONE),
                new IOException("boum"));
        }
        assertCompletionListeners(task, totalShards, totalShards, numSkippedShards, numShards, true);
        ((AsyncSearchTask.Listener)task.getProgressListener()).onFailure(new IOException("boum"));
        assertCompletionListeners(task, totalShards, totalShards, numSkippedShards, numShards, true);
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
        assertCompletionListeners(task, totalShards, 0, numSkippedShards, 0, true);
    }

    private static SearchResponse newSearchResponse(int totalShards, int successfulShards, int skippedShards,
            ShardSearchFailure... failures) {
        InternalSearchResponse response = new InternalSearchResponse(SearchHits.empty(),
            InternalAggregations.EMPTY, null, null, false, null, 1);
        return new SearchResponse(response, null, totalShards, successfulShards, skippedShards,
            100, failures, SearchResponse.Clusters.EMPTY);
    }

    private static void assertCompletionListeners(AsyncSearchTask task,
                                           int expectedTotalShards,
                                           int expectedSuccessfulShards,
                                           int expectedSkippedShards,
                                           int expectedShardFailures,
                                           boolean isPartial) throws InterruptedException {
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
