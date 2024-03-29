/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.Matchers.startsWith;

/**
 * Tests to verify that system indices are bypassing user-space thread pools
 *
 * <p>We can block thread pools by setting them to one thread and no queue, then submitting
 * threads that wait on a countdown latch. This lets us verify that operations on system indices
 * are being directed to other thread pools.</p>
 *
 * <p>When implementing this class, don't forget to override {@link ESSingleNodeTestCase#getPlugins()} if
 * the relevant system index is defined in a plugin.</p>
 */
public abstract class SystemIndexThreadPoolTests extends ESSingleNodeTestCase {

    private static final String USER_INDEX = "user_index";

    // For system indices that use ExecutorNames.CRITICAL_SYSTEM_INDEX_THREAD_POOLS, we'll want to
    // block normal system index thread pools as well.
    protected Set<String> threadPoolsToBlock() {
        return Set.of(ThreadPool.Names.GET, ThreadPool.Names.WRITE, ThreadPool.Names.SEARCH);
    }

    @Override
    protected Settings nodeSettings() {
        var settingsBuilder = Settings.builder().put(super.nodeSettings()).put("node.name", "kibana-thread-pool-tests");
        for (String threadPoolName : threadPoolsToBlock()) {
            settingsBuilder.put("thread_pool." + threadPoolName + ".size", 1);
            settingsBuilder.put("thread_pool." + threadPoolName + ".queue_size", 0);
        }
        return settingsBuilder.build();
    }

    private CountDownLatch blockThreads() {
        CountDownLatch latch = new CountDownLatch(1);
        for (String threadPoolName : threadPoolsToBlock()) {
            node().injector().getInstance(ThreadPool.class).executor(threadPoolName).execute(() -> {
                try {
                    latch.await();
                } catch (final InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        return latch;
    }

    private void assertThreadPoolsBlocked() {
        var e1 = expectThrows(
            EsRejectedExecutionException.class,
            () -> client().prepareIndex(USER_INDEX).setSource(Map.of("foo", "bar")).get()
        );
        assertThat(e1.getMessage(), startsWith("rejected execution of TimedRunnable"));
        var e2 = expectThrows(EsRejectedExecutionException.class, () -> client().prepareGet(USER_INDEX, "id").get());
        assertThat(e2.getMessage(), startsWith("rejected execution of ActionRunnable"));
        var e3 = expectThrows(
            SearchPhaseExecutionException.class,
            () -> client().prepareSearch(USER_INDEX).setQuery(QueryBuilders.matchAllQuery()).get()
        );
        assertThat(e3.getMessage(), startsWith("all shards failed"));
    }

    protected void assertRunsWithThreadPoolsBlocked(Runnable runnable) {
        client().admin().indices().prepareCreate(USER_INDEX).get();
        CountDownLatch latch = blockThreads();
        try {
            assertThreadPoolsBlocked();
            runnable.run();
        } finally {
            latch.countDown();
        }
    }
}
