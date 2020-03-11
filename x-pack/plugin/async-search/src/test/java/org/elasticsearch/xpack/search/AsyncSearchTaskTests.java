/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchShard;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;

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

    public void testWaitForInit() throws InterruptedException {
        AsyncSearchTask task = new AsyncSearchTask(0L, "", "", new TaskId("node1", 0), TimeValue.timeValueHours(1),
            Collections.emptyMap(), Collections.emptyMap(), new AsyncSearchId("0", new TaskId("node1", 1)),
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

        List<Thread> threads = new ArrayList<>();
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
            threads.add(thread);
            thread.start();
        }
        assertFalse(latch.await(numThreads*2, TimeUnit.MILLISECONDS));
        task.getProgressListener().onListShards(shards, skippedShards, SearchResponse.Clusters.EMPTY, false);
        latch.await();
    }

    public void testWithFailure() throws InterruptedException {
        AsyncSearchTask task = new AsyncSearchTask(0L, "", "", new TaskId("node1", 0), TimeValue.timeValueHours(1),
            Collections.emptyMap(), Collections.emptyMap(), new AsyncSearchId("0", new TaskId("node1", 1)),
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

        List<Thread> threads = new ArrayList<>();
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
            threads.add(thread);
            thread.start();
        }
        assertFalse(latch.await(numThreads*2, TimeUnit.MILLISECONDS));
        task.getProgressListener().onFailure(new Exception("boom"));
        latch.await();
    }

    public void testWaitForCompletion() throws InterruptedException {
        AsyncSearchTask task = new AsyncSearchTask(0L, "", "", new TaskId("node1", 0), TimeValue.timeValueHours(1),
            Collections.emptyMap(), Collections.emptyMap(), new AsyncSearchId("0", new TaskId("node1", 1)),
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

        int numShardFailures = 0;
        task.getProgressListener().onListShards(shards, skippedShards, SearchResponse.Clusters.EMPTY, false);
        for (int i = 0; i < numShards; i++) {
            task.getProgressListener().onPartialReduce(shards.subList(i, i+1),
                new TotalHits(0, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO), null, 0);
            assertCompletionListeners(task, numShards+numSkippedShards, numSkippedShards, numShardFailures, true);
        }
        task.getProgressListener().onReduce(shards,
            new TotalHits(0, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO), null, 0);
        assertCompletionListeners(task, numShards+numSkippedShards, numSkippedShards, numShardFailures, true);
        task.getProgressListener().onResponse(newSearchResponse(numShards+numSkippedShards, numShards, numSkippedShards));
        assertCompletionListeners(task, numShards+numSkippedShards,
            numSkippedShards, numShardFailures, false);
        threadPool.shutdownNow();
    }

    private SearchResponse newSearchResponse(int totalShards, int successfulShards, int skippedShards) {
        InternalSearchResponse response = new InternalSearchResponse(SearchHits.empty(),
            InternalAggregations.EMPTY, null, null, false, null, 1);
        return new SearchResponse(response, null, totalShards, successfulShards, skippedShards,
            100, ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY);
    }

    private void assertCompletionListeners(AsyncSearchTask task,
                                           int expectedTotalShards,
                                           int expectedSkippedShards,
                                           int expectedShardFailures,
                                           boolean isPartial) throws InterruptedException {
        List<Thread> threads = new ArrayList<>();
        int numThreads = randomIntBetween(1, 10);
        CountDownLatch latch = new CountDownLatch(numThreads);
        for (int i = 0; i < numThreads; i++) {
            Thread thread = new Thread(() -> task.addCompletionListener(new ActionListener<>() {
                @Override
                public void onResponse(AsyncSearchResponse resp) {
                    assertThat(resp.getSearchResponse().getTotalShards(), equalTo(expectedTotalShards));
                    assertThat(resp.getSearchResponse().getSkippedShards(), equalTo(expectedSkippedShards));
                    assertThat(resp.getSearchResponse().getFailedShards(), equalTo(expectedShardFailures));
                    assertThat(resp.isPartial(), equalTo(isPartial));
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    throw new AssertionError(e);
                }
            }, TimeValue.timeValueMillis(1)));
            threads.add(thread);
            thread.start();
        }
        latch.await();
    }
}
