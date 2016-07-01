/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.Retry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CyclicBarrier;

import static org.elasticsearch.index.reindex.ReindexTestCase.matcher;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

/**
 * Integration test for retry behavior. Useful because retrying relies on the way that the rest of Elasticsearch throws exceptions and unit
 * tests won't verify that.
 */
public class RetryTests extends ESSingleNodeTestCase {
    private static final int DOC_COUNT = 20;

    private List<CyclicBarrier> blockedExecutors = new ArrayList<>();

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(ReindexPlugin.class);
    }

    /**
     * Lower the queue sizes to be small enough that both bulk and searches will time out and have to be retried.
     */
    @Override
    protected Settings nodeSettings() {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings());
        // Use pools of size 1 so we can block them
        settings.put("thread_pool.bulk.size", 1);
        settings.put("thread_pool.search.size", 1);
        // Use queues of size 1 because size 0 is broken and because search requests need the queue to function
        settings.put("thread_pool.bulk.queue_size", 1);
        settings.put("thread_pool.search.queue_size", 1);
        return settings.build();
    }

    @Before
    public void setupSourceIndex() throws Exception {
        createIndex("source");
        // Build the test data. Don't use indexRandom because that won't work consistently with such small thread pools.
        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < DOC_COUNT; i++) {
            bulk.add(client().prepareIndex("source", "test").setSource("foo", "bar " + i));
        }
        Retry retry = Retry.on(EsRejectedExecutionException.class).policy(BackoffPolicy.exponentialBackoff());
        BulkResponse response = retry.withSyncBackoff(client(), bulk.request());
        assertFalse(response.buildFailureMessage(), response.hasFailures());
        client().admin().indices().prepareRefresh("source").get();
    }

    @After
    public void forceUnblockAllExecutors() {
        for (CyclicBarrier barrier: blockedExecutors) {
            barrier.reset();
        }
    }

    public void testReindex() throws Exception {
        testCase(ReindexAction.NAME, ReindexAction.INSTANCE.newRequestBuilder(client()).source("source").destination("dest"),
                matcher().created(DOC_COUNT));
    }

    public void testUpdateByQuery() throws Exception {
        testCase(UpdateByQueryAction.NAME, UpdateByQueryAction.INSTANCE.newRequestBuilder(client()).source("source"),
                matcher().updated(DOC_COUNT));
    }

    public void testDeleteByQuery() throws Exception {
        testCase(DeleteByQueryAction.NAME, DeleteByQueryAction.INSTANCE.newRequestBuilder(client()).source("source"),
                matcher().deleted(DOC_COUNT));
    }

    private void testCase(String action, AbstractBulkByScrollRequestBuilder<?, ?> request, BulkIndexByScrollResponseMatcher matcher)
            throws Exception {
        logger.info("Blocking search");
        CyclicBarrier initialSearchBlock = blockExecutor(ThreadPool.Names.SEARCH);

        // Make sure we use more than one batch so we have to scroll
        request.source().setSize(DOC_COUNT / randomIntBetween(2, 10));

        logger.info("Starting request");
        ListenableActionFuture<BulkIndexByScrollResponse> responseListener = request.execute();

        logger.info("Waiting for search rejections on the initial search");
        assertBusy(() -> assertThat(taskStatus(action).getSearchRetries(), greaterThan(0L)));

        logger.info("Blocking bulk and unblocking search so we start to get bulk rejections");
        CyclicBarrier bulkBlock = blockExecutor(ThreadPool.Names.BULK);
        initialSearchBlock.await();

        logger.info("Waiting for bulk rejections");
        assertBusy(() -> assertThat(taskStatus(action).getBulkRetries(), greaterThan(0L)));

        // Keep a copy of the current number of search rejections so we can assert that we get more when we block the scroll
        long initialSearchRejections = taskStatus(action).getSearchRetries();

        logger.info("Blocking search and unblocking bulk so we should get search rejections for the scroll");
        CyclicBarrier scrollBlock = blockExecutor(ThreadPool.Names.SEARCH);
        bulkBlock.await();

        logger.info("Waiting for search rejections for the scroll");
        assertBusy(() -> assertThat(taskStatus(action).getSearchRetries(), greaterThan(initialSearchRejections)));

        logger.info("Unblocking the scroll");
        scrollBlock.await();

        logger.info("Waiting for the request to finish");
        BulkIndexByScrollResponse response = responseListener.get();
        assertThat(response, matcher);
        assertThat(response.getBulkRetries(), greaterThan(0L));
        assertThat(response.getSearchRetries(), greaterThan(initialSearchRejections));
    }

    /**
     * Blocks the named executor by getting its only thread running a task blocked on a CyclicBarrier and fills the queue with a noop task.
     * So requests to use this queue should get {@link EsRejectedExecutionException}s.
     */
    private CyclicBarrier blockExecutor(String name) throws Exception {
        ThreadPool threadPool = getInstanceFromNode(ThreadPool.class);
        CyclicBarrier barrier = new CyclicBarrier(2);
        logger.info("Blocking the [{}] executor", name);
        threadPool.executor(name).execute(() -> {
            try {
                threadPool.executor(name).execute(() -> {});
                barrier.await();
                logger.info("Blocked the [{}] executor", name);
                barrier.await();
                logger.info("Ublocking the [{}] executor", name);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        barrier.await();
        blockedExecutors.add(barrier);
        return barrier;
    }

    /**
     * Fetch the status for a task of type "action". Fails if there aren't exactly one of that type of task running.
     */
    private BulkByScrollTask.Status taskStatus(String action) {
        ListTasksResponse response = client().admin().cluster().prepareListTasks().setActions(action).setDetailed(true).get();
        assertThat(response.getTasks(), hasSize(1));
        return (BulkByScrollTask.Status) response.getTasks().get(0).getStatus();
    }
}
