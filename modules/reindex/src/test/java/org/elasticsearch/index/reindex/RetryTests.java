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

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.Retry;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Netty4Plugin;
import org.junit.After;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.function.Function;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.index.reindex.ReindexTestCase.matcher;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

/**
 * Integration test for retry behavior. Useful because retrying relies on the way that the
 * rest of Elasticsearch throws exceptions and unit tests won't verify that.
 */
public class RetryTests extends ESIntegTestCase {

    private static final int DOC_COUNT = 20;

    private List<CyclicBarrier> blockedExecutors = new ArrayList<>();

    @After
    public void forceUnblockAllExecutors() {
        for (CyclicBarrier barrier: blockedExecutors) {
            barrier.reset();
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(
                ReindexPlugin.class,
                Netty4Plugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Arrays.asList(
                ReindexPlugin.class,
                Netty4Plugin.class);
    }

    /**
     * Lower the queue sizes to be small enough that both bulk and searches will time out and have to be retried.
     */
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put(nodeSettings()).build();
    }

    final Settings nodeSettings() {
        return Settings.builder()
                // enable HTTP so we can test retries on reindex from remote; in this case the "remote" cluster is just this cluster
                .put(NetworkModule.HTTP_ENABLED.getKey(), true)
                // whitelist reindexing from the HTTP host we're going to use
                .put(TransportReindexAction.REMOTE_CLUSTER_WHITELIST.getKey(), "127.0.0.1:*")
                .build();
    }

    public void testReindex() throws Exception {
        testCase(
                ReindexAction.NAME,
                client -> ReindexAction.INSTANCE.newRequestBuilder(client).source("source").destination("dest"),
                matcher().created(DOC_COUNT));
    }

    public void testReindexFromRemote() throws Exception {
        Function<Client, AbstractBulkByScrollRequestBuilder<?, ?>> function = client -> {
            /*
             * Use the master node for the reindex from remote because that node
             * doesn't have a copy of the data on it.
             */
            NodeInfo masterNode = null;
            for (NodeInfo candidate : client.admin().cluster().prepareNodesInfo().get().getNodes()) {
                if (candidate.getNode().isMasterNode()) {
                    masterNode = candidate;
                }
            }
            assertNotNull(masterNode);

            TransportAddress address = masterNode.getHttp().getAddress().publishAddress();
            RemoteInfo remote = new RemoteInfo("http", address.getAddress(), address.getPort(), new BytesArray("{\"match_all\":{}}"), null,
                    null, emptyMap(), RemoteInfo.DEFAULT_SOCKET_TIMEOUT, RemoteInfo.DEFAULT_CONNECT_TIMEOUT);
            ReindexRequestBuilder request = ReindexAction.INSTANCE.newRequestBuilder(client).source("source").destination("dest")
                    .setRemoteInfo(remote);
            return request;
        };
        testCase(ReindexAction.NAME, function, matcher().created(DOC_COUNT));
    }

    public void testUpdateByQuery() throws Exception {
        testCase(UpdateByQueryAction.NAME, client -> UpdateByQueryAction.INSTANCE.newRequestBuilder(client).source("source"),
                matcher().updated(DOC_COUNT));
    }

    public void testDeleteByQuery() throws Exception {
        testCase(DeleteByQueryAction.NAME, client -> DeleteByQueryAction.INSTANCE.newRequestBuilder(client).source("source")
                .filter(QueryBuilders.matchAllQuery()), matcher().deleted(DOC_COUNT));
    }

    private void testCase(
            String action,
            Function<Client, AbstractBulkByScrollRequestBuilder<?, ?>> request,
            BulkIndexByScrollResponseMatcher matcher)
            throws Exception {
        /*
         * These test cases work by stuffing the search and bulk queues of a single node and
         * making sure that we read and write from that node. Because of some "fun" with the
         * way that searches work, we need at least one more node to act as the coordinating
         * node for the search request. If we didn't do this then the searches would get stuck
         * in the queue anyway because we force queue portions of the coordinating node's
         * actions. This is not a big deal in normal operations but a real pain when you are
         * intentionally stuffing queues hoping for a failure.
         */

        final Settings nodeSettings = Settings.builder()
                // use pools of size 1 so we can block them
                .put("thread_pool.bulk.size", 1)
                .put("thread_pool.search.size", 1)
                // use queues of size 1 because size 0 is broken and because search requests need the queue to function
                .put("thread_pool.bulk.queue_size", 1)
                .put("thread_pool.search.queue_size", 1)
                .put("node.attr.color", "blue")
                .build();
        final String node = internalCluster().startDataOnlyNode(nodeSettings);
        final Settings indexSettings =
                Settings.builder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                        .put("index.routing.allocation.include.color", "blue")
                        .build();

        // Create the source index on the node with small thread pools so we can block them.
        client().admin().indices().prepareCreate("source").setSettings(indexSettings).execute().actionGet();
        // Not all test cases use the dest index but those that do require that it be on the node will small thread pools
        client().admin().indices().prepareCreate("dest").setSettings(indexSettings).execute().actionGet();
        // Build the test data. Don't use indexRandom because that won't work consistently with such small thread pools.
        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < DOC_COUNT; i++) {
            bulk.add(client().prepareIndex("source", "test").setSource("foo", "bar " + i));
        }

        Retry retry = new Retry(EsRejectedExecutionException.class, BackoffPolicy.exponentialBackoff(), client().threadPool());
        BulkResponse initialBulkResponse = retry.withBackoff(client()::bulk, bulk.request(), client().settings()).actionGet();
        assertFalse(initialBulkResponse.buildFailureMessage(), initialBulkResponse.hasFailures());
        client().admin().indices().prepareRefresh("source").get();

        logger.info("Blocking search");
        CyclicBarrier initialSearchBlock = blockExecutor(ThreadPool.Names.SEARCH, node);

        AbstractBulkByScrollRequestBuilder<?, ?> builder = request.apply(internalCluster().masterClient());
        // Make sure we use more than one batch so we have to scroll
        builder.source().setSize(DOC_COUNT / randomIntBetween(2, 10));

        logger.info("Starting request");
        ActionFuture<BulkByScrollResponse> responseListener = builder.execute();

        try {
            logger.info("Waiting for search rejections on the initial search");
            assertBusy(() -> assertThat(taskStatus(action).getSearchRetries(), greaterThan(0L)));

            logger.info("Blocking bulk and unblocking search so we start to get bulk rejections");
            CyclicBarrier bulkBlock = blockExecutor(ThreadPool.Names.BULK, node);
            initialSearchBlock.await();

            logger.info("Waiting for bulk rejections");
            assertBusy(() -> assertThat(taskStatus(action).getBulkRetries(), greaterThan(0L)));

            // Keep a copy of the current number of search rejections so we can assert that we get more when we block the scroll
            long initialSearchRejections = taskStatus(action).getSearchRetries();

            logger.info("Blocking search and unblocking bulk so we should get search rejections for the scroll");
            CyclicBarrier scrollBlock = blockExecutor(ThreadPool.Names.SEARCH, node);
            bulkBlock.await();

            logger.info("Waiting for search rejections for the scroll");
            assertBusy(() -> assertThat(taskStatus(action).getSearchRetries(), greaterThan(initialSearchRejections)));

            logger.info("Unblocking the scroll");
            scrollBlock.await();

            logger.info("Waiting for the request to finish");
            BulkByScrollResponse response = responseListener.get();
            assertThat(response, matcher);
            assertThat(response.getBulkRetries(), greaterThan(0L));
            assertThat(response.getSearchRetries(), greaterThan(initialSearchRejections));
        } finally {
            // Fetch the response just in case we blew up half way through. This will make sure the failure is thrown up to the top level.
            BulkByScrollResponse response = responseListener.get();
            assertThat(response.getSearchFailures(), empty());
            assertThat(response.getBulkFailures(), empty());
        }
    }

    /**
     * Blocks the named executor by getting its only thread running a task blocked on a CyclicBarrier and fills the queue with a noop task.
     * So requests to use this queue should get {@link EsRejectedExecutionException}s.
     */
    private CyclicBarrier blockExecutor(String name, String node) throws Exception {
        ThreadPool threadPool = internalCluster().getInstance(ThreadPool.class, node);
        CyclicBarrier barrier = new CyclicBarrier(2);
        logger.info("Blocking the [{}] executor", name);
        threadPool.executor(name).execute(() -> {
            try {
                threadPool.executor(name).execute(() -> {});
                barrier.await();
                logger.info("Blocked the [{}] executor", name);
                barrier.await();
                logger.info("Unblocking the [{}] executor", name);
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
        /*
         * We always use the master client because we always start the test requests on the
         * master. We do this simply to make sure that the test request is not started on the
         * node who's queue we're manipulating.
         */
        ListTasksResponse response = client().admin().cluster().prepareListTasks().setActions(action).setDetailed(true).get();
        assertThat(response.getTasks(), hasSize(1));
        return (BulkByScrollTask.Status) response.getTasks().get(0).getStatus();
    }

}
