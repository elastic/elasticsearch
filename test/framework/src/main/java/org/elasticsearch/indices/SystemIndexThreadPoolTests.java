/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Phaser;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
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
public abstract class SystemIndexThreadPoolTests extends ESIntegTestCase {

    private static final String USER_INDEX = "user_index";

    // For system indices that use ExecutorNames.CRITICAL_SYSTEM_INDEX_THREAD_POOLS, we'll want to
    // block normal system index thread pools as well.
    protected Set<String> threadPoolsToBlock() {
        return Set.of(ThreadPool.Names.GET, ThreadPool.Names.WRITE, ThreadPool.Names.SEARCH);
    }

    private void assertThreadPoolsBlocked() {
        TimeValue timeout = TimeValue.timeValueMillis(25);
        logger.info("cluster data nodes: " + cluster().numDataNodes() + ", data and master: " + cluster().numDataAndMasterNodes());
        var e1 = expectThrows(
            ElasticsearchTimeoutException.class,
            () -> client().prepareIndex(USER_INDEX).setSource(Map.of("foo", "bar")).get(timeout)
        );
        assertThat(e1.getMessage(), startsWith("java.util.concurrent.TimeoutException: Timeout waiting for task."));
        var e2 = expectThrows(ElasticsearchTimeoutException.class, () -> client().prepareGet(USER_INDEX, "id").get(timeout));
        assertThat(e2.getMessage(), startsWith("java.util.concurrent.TimeoutException: Timeout waiting for task."));
        var e3 = expectThrows(
            ElasticsearchTimeoutException.class,
            () -> client().prepareSearch(USER_INDEX).setQuery(QueryBuilders.matchAllQuery()).get(timeout)
        );
        assertThat(e3.getMessage(), startsWith("java.util.concurrent.TimeoutException: Timeout waiting for task."));
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

    public void testUserThreadPoolsAreBlocked() {
        assertAcked(client().admin().indices().prepareCreate(USER_INDEX));

        runWithBlockedThreadPools(this::assertThreadPoolsBlocked);

        assertAcked(client().admin().indices().prepareDelete(USER_INDEX));
    }
}
