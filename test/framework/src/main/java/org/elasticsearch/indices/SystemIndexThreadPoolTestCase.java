/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Phaser;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;

/**
 * Tests to verify that system indices are bypassing user-space thread pools
 *
 * <p>We can block thread pools by setting them to one thread and no queue, then submitting
 * threads that wait on a countdown latch. This lets us verify that operations on system indices
 * are being directed to other thread pools.</p>
 *
 * <p>When implementing this class, don't forget to override {@link ESIntegTestCase#nodePlugins()} if
 * the relevant system index is defined in a plugin.</p>
 */
public abstract class SystemIndexThreadPoolTestCase extends ESIntegTestCase {

    private static final String USER_INDEX = "user_index";

    // For system indices that use ExecutorNames.CRITICAL_SYSTEM_INDEX_THREAD_POOLS, we'll want to
    // block normal system index thread pools as well.
    protected Set<String> threadPoolsToBlock() {
        return Set.of(ThreadPool.Names.GET, ThreadPool.Names.WRITE, ThreadPool.Names.SEARCH);
    }

    protected void runWithBlockedThreadPools(Runnable runnable) {
        Phaser phaser = new Phaser();
        Runnable waitAction = () -> {
            phaser.arriveAndAwaitAdvance();
            phaser.arriveAndAwaitAdvance();
        };
        phaser.register(); // register this test's thread

        for (String nodeName : internalCluster().getNodeNames()) {
            ThreadPool threadPool = internalCluster().getInstance(ThreadPool.class, nodeName);
            for (String threadPoolName : threadPoolsToBlock()) {
                ThreadPool.Info info = threadPool.info(threadPoolName);
                phaser.bulkRegister(info.getMax());
                for (int i = 0; i < info.getMax(); i++) {
                    threadPool.executor(threadPoolName).submit(waitAction);
                }
            }
        }
        phaser.arriveAndAwaitAdvance();
        try {
            runnable.run();
        } finally {
            phaser.arriveAndAwaitAdvance();
        }
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/107625")
    public void testUserThreadPoolsAreBlocked() {
        assertAcked(client().admin().indices().prepareCreate(USER_INDEX));

        runWithBlockedThreadPools(this::assertThreadPoolsBlocked);

        assertAcked(client().admin().indices().prepareDelete(USER_INDEX));
    }

    private void assertThreadPoolsBlocked() {
        fillThreadPoolQueues(); // rejections are easier to check than timeouts

        var e1 = expectThrows(
            EsRejectedExecutionException.class,
            () -> client().prepareIndex(USER_INDEX).setSource(Map.of("foo", "bar")).get()
        );
        assertThat(e1.getMessage(), startsWith("rejected execution of TimedRunnable"));
        var e2 = expectThrows(EsRejectedExecutionException.class, () -> client().prepareGet(USER_INDEX, "id").get());
        assertThat(e2.getMessage(), startsWith("rejected execution of ActionRunnable"));
        var e3 = expectThrows(
            SearchPhaseExecutionException.class,
            () -> client().prepareSearch(USER_INDEX)
                .setQuery(QueryBuilders.matchAllQuery())
                // Request times out if max concurrent shard requests is set to 1
                .setMaxConcurrentShardRequests(usually() ? SearchRequest.DEFAULT_MAX_CONCURRENT_SHARD_REQUESTS : randomIntBetween(2, 10))
                .get()
        );
        assertThat(e3.getMessage(), containsString("all shards failed"));
    }

    private void fillThreadPoolQueues() {
        for (String nodeName : internalCluster().getNodeNames()) {
            ThreadPool threadPool = internalCluster().getInstance(ThreadPool.class, nodeName);
            for (String threadPoolName : threadPoolsToBlock()) {
                ThreadPool.Info info = threadPool.info(threadPoolName);

                // fill up the queue
                for (int i = 0; i < info.getQueueSize().singles(); i++) {
                    try {
                        threadPool.executor(threadPoolName).submit(() -> {});
                    } catch (EsRejectedExecutionException e) {
                        // we can't be sure that some other task won't get queued in a test cluster
                        // but we should put all the tasks in there anyway
                    }
                }
            }
        }
    }
}
