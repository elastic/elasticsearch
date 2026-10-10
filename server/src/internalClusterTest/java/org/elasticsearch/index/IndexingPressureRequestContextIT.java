/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.stats.IndexingPressureStats;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Verifies that the request context a write retains while in flight (its request headers, and any
 * {@link org.apache.lucene.util.Accountable} transient value) counts toward indexing pressure, once per copy held on the heap:
 * on the coordinating node, on a primary that received the request over the network, and on a replica. A primary that is also the
 * coordinating node does not count it again, because the local reroute reuses the coordinating request's context.
 *
 * <p>A large request header stands in for the security metadata an authenticated client contributes. The accounting is generic, so
 * these tests need no security.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 2, numClientNodes = 1)
public class IndexingPressureRequestContextIT extends ESIntegTestCase {

    private static final String INDEX_NAME = "request-context";
    private static final String LARGE_HEADER = "x-request-context-test";
    private static final int HEADER_LENGTH = 50_000;
    private static final ByteSizeValue PRIMARY_LIMIT = ByteSizeValue.ofKb(200);

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            // writes held in flight must queue rather than be rejected for lack of queue capacity
            .put("thread_pool.write.queue_size", -1)
            .put(IndexingPressure.MAX_PRIMARY_BYTES.getKey(), PRIMARY_LIMIT)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(HoldIndexingPlugin.class);
    }

    @After
    public void releaseHeldIndexing() {
        HoldIndexingPlugin.release();
    }

    /**
     * The coordinating node and the primary node are different, so the request crosses the network and each node holds its own copy
     * of the context. Each must count it: the coordinator as part of the coordinating reservation, the primary as part of the
     * network-received primary reservation.
     */
    public void testNetworkReceivedRequestCountsContextOnCoordinatorAndPrimary() throws Exception {
        final String primaryNode = createIndexAndGetPrimaryNode();
        final String coordinatingNode = coordinatingOnlyNode();
        final IndexingPressure coordinatingPressure = internalCluster().getInstance(IndexingPressure.class, coordinatingNode);
        final IndexingPressure primaryPressure = internalCluster().getInstance(IndexingPressure.class, primaryNode);
        final String header = randomAlphaOfLength(HEADER_LENGTH);
        final BulkRequest bulk = oneDocumentBulk();
        final long payload = bulk.ramBytesUsed();

        HoldIndexingPlugin.hold();
        final ActionFuture<BulkResponse> response = client(coordinatingNode).filterWithHeader(Map.of(LARGE_HEADER, header)).bulk(bulk);

        assertBusy(() -> {
            assertThat(coordinatingPressure.stats().getCurrentCoordinatingBytes(), greaterThanOrEqualTo(payload + header.length()));
            assertThat(primaryPressure.stats().getCurrentPrimaryBytes(), greaterThanOrEqualTo((long) header.length()));
        });

        HoldIndexingPlugin.release();
        assertNoFailures(response.actionGet());
        assertBusy(() -> {
            assertThat(coordinatingPressure.stats().getCurrentCoordinatingBytes(), equalTo(0L));
            assertThat(primaryPressure.stats().getCurrentPrimaryBytes(), equalTo(0L));
        });
    }

    /**
     * The coordinating node is also the primary node, so there is one context and no network hop. The primary phase reuses the
     * coordinating request's context through a local reroute, so counting it again there would charge the same heap twice.
     */
    public void testLocalPrimaryDoesNotCountTheSameContextTwice() throws Exception {
        final String primaryNode = createIndexAndGetPrimaryNode();
        final IndexingPressure pressure = internalCluster().getInstance(IndexingPressure.class, primaryNode);
        final String header = randomAlphaOfLength(HEADER_LENGTH);
        final BulkRequest bulk = oneDocumentBulk();
        final long payload = bulk.ramBytesUsed();

        HoldIndexingPlugin.hold();
        final ActionFuture<BulkResponse> response = client(primaryNode).filterWithHeader(Map.of(LARGE_HEADER, header)).bulk(bulk);

        // the primary operation has started (and so holds its primary reservation) once it is counted among the primary ops
        assertBusy(() -> {
            assertThat(pressure.stats().getCurrentCoordinatingBytes(), greaterThanOrEqualTo(payload + header.length()));
            assertThat(pressure.stats().getCurrentPrimaryOps(), greaterThan(0L));
        });
        final IndexingPressureStats held = pressure.stats();
        // the primary phase added only the shard request, not another copy of the context
        assertThat(held.getCurrentPrimaryBytes(), lessThan(header.length() / 2L));
        // and the node as a whole is charged for one context, not two
        assertThat(held.getCurrentCombinedCoordinatingAndPrimaryBytes(), lessThan(2L * header.length()));

        HoldIndexingPlugin.release();
        assertNoFailures(response.actionGet());
        assertBusy(() -> {
            assertThat(pressure.stats().getCurrentCoordinatingBytes(), equalTo(0L));
            assertThat(pressure.stats().getCurrentPrimaryBytes(), equalTo(0L));
        });
    }

    /**
     * Many small requests, each retaining a large context, queue on a primary node. The payload of all of them together is far below
     * the primary limit, so only the contexts can reach it: once they do, further requests are rejected with 429 at the primary
     * check, and the admitted ones complete normally once released.
     */
    public void testPrimaryRejectsWhenRetainedContextsExceedLimit() throws Exception {
        final String primaryNode = createIndexAndGetPrimaryNode();
        final String coordinatingNode = coordinatingOnlyNode();
        final IndexingPressure primaryPressure = internalCluster().getInstance(IndexingPressure.class, primaryNode);
        final int requests = 12;
        // the payload of every request together is a small fraction of the limit, so only the contexts can reach it
        assertThat(requests * oneDocumentBulk().ramBytesUsed(), lessThan(PRIMARY_LIMIT.getBytes() / 10));
        final long rejectionsBefore = primaryPressure.stats().getPrimaryRejections();

        HoldIndexingPlugin.hold();
        final List<ActionFuture<BulkResponse>> responses = new ArrayList<>();
        for (int i = 0; i < requests; i++) {
            final String header = randomAlphaOfLength(HEADER_LENGTH);
            responses.add(client(coordinatingNode).filterWithHeader(Map.of(LARGE_HEADER, header)).bulk(oneDocumentBulk()));
        }

        // each request is either admitted (counted among the current primary ops) or rejected at the primary check
        assertBusy(() -> {
            final IndexingPressureStats stats = primaryPressure.stats();
            assertThat(stats.getPrimaryRejections() - rejectionsBefore + stats.getCurrentPrimaryOps(), equalTo((long) requests));
        });
        final IndexingPressureStats held = primaryPressure.stats();
        final long rejected = held.getPrimaryRejections() - rejectionsBefore;
        final long admitted = held.getCurrentPrimaryOps();
        assertThat(rejected, greaterThan(0L));
        assertThat(admitted, greaterThan(0L));
        assertThat(held.getCurrentPrimaryBytes(), lessThanOrEqualTo(PRIMARY_LIMIT.getBytes()));

        HoldIndexingPlugin.release();
        long succeeded = 0;
        long failedWithTooManyRequests = 0;
        for (ActionFuture<BulkResponse> future : responses) {
            for (BulkItemResponse item : future.actionGet().getItems()) {
                if (item.isFailed()) {
                    assertThat(item.getFailure().getStatus(), equalTo(RestStatus.TOO_MANY_REQUESTS));
                    failedWithTooManyRequests++;
                } else {
                    succeeded++;
                }
            }
        }
        assertThat(succeeded, equalTo(admitted));
        assertThat(failedWithTooManyRequests, equalTo(rejected));
        assertBusy(() -> assertThat(primaryPressure.stats().getCurrentPrimaryBytes(), equalTo(0L)));
    }

    /**
     * A replica request always arrives over the network, so the replica node materializes and retains its own copy of the context
     * for as long as the replica write is queued, and must count it, just as a network-received primary does. The replica node's
     * write pool is blocked so the replica write stays queued, holding its reservation.
     */
    public void testReplicaCountsContextWhileWriteIsQueued() throws Exception {
        assertAcked(prepareCreate(INDEX_NAME, indexSettings(1, 1)));
        ensureGreen(INDEX_NAME);
        final String replicaNode = replicaNodeName();
        final IndexingPressure replicaPressure = internalCluster().getInstance(IndexingPressure.class, replicaNode);
        final String header = randomAlphaOfLength(HEADER_LENGTH);

        try (Releasable blocked = blockWriteThreadPool(internalCluster().getInstance(ThreadPool.class, replicaNode))) {
            final ActionFuture<BulkResponse> response = client(coordinatingOnlyNode()).filterWithHeader(Map.of(LARGE_HEADER, header))
                .bulk(oneDocumentBulk());

            assertBusy(() -> assertThat(replicaPressure.stats().getCurrentReplicaBytes(), greaterThanOrEqualTo((long) header.length())));

            blocked.close();
            assertNoFailures(response.actionGet());
        }
        assertBusy(() -> assertThat(replicaPressure.stats().getCurrentReplicaBytes(), equalTo(0L)));
    }

    private String createIndexAndGetPrimaryNode() {
        assertAcked(prepareCreate(INDEX_NAME, indexSettings(1, 0)));
        ensureGreen(INDEX_NAME);
        final IndicesStatsResponse stats = indicesAdmin().prepareStats(INDEX_NAME).get();
        final String primaryNodeId = Stream.of(stats.getShards())
            .map(ShardStats::getShardRouting)
            .filter(ShardRouting::primary)
            .findAny()
            .get()
            .currentNodeId();
        return clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState().nodes().get(primaryNodeId).getName();
    }

    private String replicaNodeName() {
        final IndicesStatsResponse stats = indicesAdmin().prepareStats(INDEX_NAME).get();
        final String replicaNodeId = Stream.of(stats.getShards())
            .map(ShardStats::getShardRouting)
            .filter(routing -> routing.primary() == false)
            .findAny()
            .get()
            .currentNodeId();
        return clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState().nodes().get(replicaNodeId).getName();
    }

    /** Occupies every write thread of the node until the returned releasable is closed, so later writes queue behind them. */
    private static Releasable blockWriteThreadPool(ThreadPool threadPool) {
        final CountDownLatch release = new CountDownLatch(1);
        final int threads = threadPool.info(ThreadPool.Names.WRITE).getMax();
        final CountDownLatch allBlocked = new CountDownLatch(threads);
        for (int i = 0; i < threads; i++) {
            threadPool.executor(ThreadPool.Names.WRITE).execute(() -> {
                try {
                    allBlocked.countDown();
                    release.await();
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
            });
        }
        return release::countDown;
    }

    private String coordinatingOnlyNode() {
        return clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT)
            .get()
            .getState()
            .nodes()
            .getCoordinatingOnlyNodes()
            .values()
            .iterator()
            .next()
            .getName();
    }

    private static BulkRequest oneDocumentBulk() {
        return new BulkRequest().add(new IndexRequest(INDEX_NAME).source(Map.of("field", "value")));
    }

    /**
     * Blocks indexing into the test index, on whatever node it runs, until released. The blocked primary operation keeps its
     * reservations, and the thread context of every request queued behind it, in flight, as a saturated write queue would.
     */
    public static class HoldIndexingPlugin extends Plugin {
        private static volatile CountDownLatch holdLatch = new CountDownLatch(0);

        static void hold() {
            holdLatch = new CountDownLatch(1);
        }

        static void release() {
            holdLatch.countDown();
        }

        @Override
        public void onIndexModule(IndexModule indexModule) {
            indexModule.addIndexOperationListener(new IndexingOperationListener() {
                @Override
                public Engine.Index preIndex(ShardId shardId, Engine.Index operation) {
                    if (INDEX_NAME.equals(shardId.getIndexName())) {
                        try {
                            if (holdLatch.await(30, TimeUnit.SECONDS) == false) {
                                throw new AssertionError("timed out waiting for the held indexing operation to be released");
                            }
                        } catch (InterruptedException e) {
                            throw new AssertionError(e);
                        }
                    }
                    return operation;
                }
            });
        }
    }
}
