/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;

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
            settingsBuilder.put("thread_pool." + threadPoolName + ".queue_size", 10);
        }
        return settingsBuilder.build();
    }

    private void assertThreadPoolsBlocked() {
        TimeValue timeout = TimeValue.timeValueMillis(25);
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
        ThreadPool threadPool = node().injector().getInstance(ThreadPool.class);
        int numThreadsToBlock = threadPoolsToBlock().stream().map(threadPool::info).mapToInt(ThreadPool.Info::getMax).sum();
        CyclicBarrier cb = new CyclicBarrier(numThreadsToBlock + 1);
        Runnable waitAction = () -> {
            safeAwait(cb);
            safeAwait(cb);
        };
        for (String threadPoolName : threadPoolsToBlock()) {
            ThreadPool.Info info = threadPool.info(threadPoolName);
            for (int i = 0; i < info.getMax(); i++) {
                threadPool.executor(threadPoolName).submit(waitAction);
            }
        }
        safeAwait(cb);
        try {
            runnable.run();
        } finally {
            safeAwait(cb);
        }
    }

    public void testUserThreadPoolsAreBlocked() {
        client().admin().indices().prepareCreate(USER_INDEX).get();

        runWithBlockedThreadPools(this::assertThreadPoolsBlocked);

        client().admin().indices().prepareDelete(USER_INDEX).get();
    }
}
