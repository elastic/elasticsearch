/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.kibana;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolStats;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Phaser;
import java.util.stream.Stream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;

/**
 * Tests to verify that system indices are bypassing user-space thread pools
 *
 * <p>We can block thread pools by setting them to one thread and 1 element queue, then submitting
 * threads that wait on a phaser. This lets us verify that operations on system indices
 * are being directed to other thread pools.</p>
 */
public class KibanaThreadPoolIT extends ESIntegTestCase {
    private static final Logger logger = LogManager.getLogger(KibanaThreadPoolIT.class);

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(IndexingPressure.MAX_PRIMARY_BYTES.getKey(), "1KB")
            .put(IndexingPressure.MAX_COORDINATING_BYTES.getKey(), "1KB")
            .put("thread_pool.search.size", 1)
            .put("thread_pool.search.queue_size", 1)
            .put("thread_pool.write.size", 1)
            .put("thread_pool.write.queue_size", 1)
            .put("thread_pool.get.size", 1)
            .put("thread_pool.get.queue_size", 1)
            // a rejected GET may retry on an INITIALIZING shard (the target of a relocation) and unexpectedly succeed, so block rebalancing
            .put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Rebalance.NONE)
            .build();
    }

    private static final String USER_INDEX = "user_index";
    // For system indices that use ExecutorNames.CRITICAL_SYSTEM_INDEX_THREAD_POOLS, we'll want to
    // block normal system index thread pools as well.
    private static final Set<String> THREAD_POOLS_TO_BLOCK = Set.of(ThreadPool.Names.GET, ThreadPool.Names.WRITE, ThreadPool.Names.SEARCH);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Set.of(KibanaPlugin.class);
    }

    public void testKibanaThreadPoolByPassesBlockedThreadPools() throws Exception {
        List<String> kibanaSystemIndices = Stream.of(
            KibanaPlugin.KIBANA_INDEX_DESCRIPTOR.getIndexPattern(),
            KibanaPlugin.REPORTING_INDEX_DESCRIPTOR.getIndexPattern(),
            KibanaPlugin.APM_AGENT_CONFIG_INDEX_DESCRIPTOR.getIndexPattern(),
            KibanaPlugin.APM_CUSTOM_LINK_INDEX_DESCRIPTOR.getIndexPattern()
        ).map(s -> s.replace("*", randomAlphaOfLength(8).toLowerCase(Locale.ROOT))).toList();

        runWithBlockedThreadPools(() -> {
            for (String index : kibanaSystemIndices) {
                // index documents
                String idToDelete = client().prepareIndex(index).setSource(Map.of("foo", "delete me!")).get().getId();
                String idToUpdate = client().prepareIndex(index).setSource(Map.of("foo", "update me!")).get().getId();

                // bulk index, delete, and update
                Client bulkClient = client();
                BulkResponse response = bulkClient.prepareBulk(index)
                    .add(bulkClient.prepareIndex(index).setSource(Map.of("foo", "search me!")))
                    .add(bulkClient.prepareDelete(index, idToDelete))
                    .add(bulkClient.prepareUpdate().setId(idToUpdate).setDoc(Map.of("foo", "I'm updated!")))
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .get();
                assertNoFailures(response);

                // match-all search
                assertHitCount(client().prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()), 2);
            }
        });
    }

    public void testBlockedThreadPoolsRejectUserRequests() throws Exception {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(USER_INDEX)
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)) // avoid retrying rejected actions
        );

        runWithBlockedThreadPools(this::assertThreadPoolsBlocked);

        assertAcked(client().admin().indices().prepareDelete(USER_INDEX));
    }

    private void assertThreadPoolsBlocked() {

        var e1 = expectThrows(
            EsRejectedExecutionException.class,
            () -> client().prepareIndex(USER_INDEX).setSource(Map.of("foo", "bar")).get()
        );
        assertThat(e1.getMessage(), startsWith("rejected execution of TimedRunnable"));

        final var getFuture = client().prepareGet(USER_INDEX, "id").execute();
        // response handling is force-executed on GET pool, so we must
        // (a) wait for that task to be enqueued, expanding the queue beyond its configured limit, and
        // (b) check for the exception in the background

        try {
            assertTrue(waitUntil(() -> {
                if (getFuture.isDone()) {
                    return true;
                }
                for (ThreadPool threadPool : internalCluster().getInstances(ThreadPool.class)) {
                    for (ThreadPoolStats.Stats stats : threadPool.stats().stats()) {
                        if (stats.name().equals(ThreadPool.Names.GET) && stats.queue() > 1) {
                            return true;
                        }
                    }
                }
                return false;
            }));
        } catch (Exception e) {
            fail(e);
        }

        new Thread(() -> expectThrows(EsRejectedExecutionException.class, () -> getFuture.actionGet(SAFE_AWAIT_TIMEOUT))).start();

        // intentionally commented out this test until https://github.com/elastic/elasticsearch/issues/97916 is fixed
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

    protected void runWithBlockedThreadPools(Runnable runnable) throws Exception {
        Phaser phaser = new Phaser();

        // register this test's thread
        phaser.register();

        blockThreadPool(phaser);
        phaser.arriveAndAwaitAdvance();// wait until all waitAction are executing

        fillQueues();

        logger.debug("number of nodes " + internalCluster().getNodeNames().length);
        logger.debug("number of parties arrived " + phaser.getArrivedParties());
        try {
            runnable.run();
        } finally {
            phaser.arriveAndAwaitAdvance(); // release all waitAction
        }
    }

    private void blockThreadPool(Phaser phaser) {
        for (String nodeName : internalCluster().getNodeNames()) {
            ThreadPool threadPool = internalCluster().getInstance(ThreadPool.class, nodeName);
            for (String threadPoolName : THREAD_POOLS_TO_BLOCK) {
                blockThreadPool(threadPoolName, threadPool, phaser);
            }
        }
    }

    private void fillQueues() {
        for (String nodeName : internalCluster().getNodeNames()) {
            ThreadPool threadPool = internalCluster().getInstance(ThreadPool.class, nodeName);
            for (String threadPoolName : THREAD_POOLS_TO_BLOCK) {
                fillThreadPoolQueues(threadPoolName, threadPool);
            }
        }
    }

    private static void blockThreadPool(String threadPoolName, ThreadPool threadPool, Phaser phaser) {
        ThreadPool.Info info = threadPool.info(threadPoolName);

        Runnable waitAction = () -> {
            phaser.arriveAndAwaitAdvance();// block until all are executed on a threadpool
            phaser.arriveAndAwaitAdvance();// block until main thread has not finished
        };

        phaser.bulkRegister(info.getMax());

        for (int i = 0; i < info.getMax(); i++) {
            // we need to make sure that there is a task blocking a thread pool
            // otherwise a queue might end up having a spot
            do {
                try {
                    threadPool.executor(threadPoolName).execute(waitAction);
                    break;
                } catch (EsRejectedExecutionException e) {
                    // if exception was thrown when submitting, retry.
                }
            } while (true);
        }
    }

    private static void fillThreadPoolQueues(String threadPoolName, ThreadPool threadPool) {
        ThreadPool.Info info = threadPool.info(threadPoolName);

        for (int i = 0; i < info.getQueueSize().singles(); i++) {
            try {
                threadPool.executor(threadPoolName).execute(() -> {});
            } catch (EsRejectedExecutionException e) {
                logger.debug("Exception when filling the queue " + threadPoolName, e);
                logThreadPoolQueue(threadPoolName, threadPool);
                // we can't be sure that some other task won't get queued in a test cluster
                // but the threadpool's thread is already blocked
            }
        }

        logThreadPoolQueue(threadPoolName, threadPool);
    }

    private static void logThreadPoolQueue(String threadPoolName, ThreadPool threadPool) {
        if (threadPool.executor(threadPoolName) instanceof EsThreadPoolExecutor tpe) {
            logger.debug("Thread pool details " + threadPoolName + " " + tpe);
            logger.debug(Arrays.toString(tpe.getTasks().toArray()));
        }
    }

}
