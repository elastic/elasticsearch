/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.disruption.BlockClusterStateProcessing;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xpack.esql.plugin.ComputeService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

/**
 * A coordinator whose cluster state lags behind a shard relocation. It sends the data node request to the node the shard has left,
 * which answers that it does not have the shard, and its own routing still shows that node as the only copy. The coordinator has to
 * wait for the routing that shows where the shard went, and not spend its resolution attempts on the copy that is gone.
 * <p>
 * The query runs from a coordinating-only node, so that holding back its cluster state does not hold back the master or a data node.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class EsqlStaleRoutingIT extends AbstractEsqlIntegTestCase {

    /** {@code DataNodeRequestSender} is package-private in {@code ...esql.plugin}, so its logger is named by string. */
    private static final String SENDER_LOGGER = "org.elasticsearch.xpack.esql.plugin.DataNodeRequestSender";

    private static final String INDEX = "test";
    private static final String QUERY = "FROM " + INDEX + " | STATS count = COUNT(*), sum = SUM(id)";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(MockTransportService.TestPlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            // short, so that the master does not wait long for the coordinator that applies no state
            .put(Coordinator.PUBLISH_TIMEOUT_SETTING.getKey(), TimeValue.timeValueSeconds(2))
            .build();
    }

    @TestLogging(value = SENDER_LOGGER + ":DEBUG", reason = "waits for the coordinator to start its routing wait before releasing it")
    public void testCoordinatorOnStaleRoutingWaitsForTheNewCopy() throws Exception {
        boolean allowPartialResults = randomBoolean();
        ParkedQuery parked = parkQueryOnStaleRouting(allowPartialResults);
        try {
            parked.unblockCoordinator();
            try (EsqlQueryResponse response = parked.query.actionGet(30, TimeUnit.SECONDS)) {
                assertFalse(response.isPartial());
                assertThat("every row exactly once", getValuesList(response), equalTo(List.of(List.of(parked.docs, parked.idSum))));
            }
            assertThat("the node the shard left is asked once", parked.dataRequestsOnOldNode.get(), equalTo(1));
            assertThat("the node the shard moved to is asked once", parked.dataRequestsOnNewNode.get(), equalTo(1));
        } finally {
            parked.unblockCoordinator();
        }
    }

    /**
     * A query that waits on its coordinator for newer routing, and what the test needs to let it go on.
     */
    private static final class ParkedQuery {
        private final String coordinator;
        private final ActionFuture<EsqlQueryResponse> query;
        private final long docs;
        private final long idSum;
        private final AtomicInteger dataRequestsOnOldNode;
        private final AtomicInteger dataRequestsOnNewNode;
        private BlockClusterStateProcessing blocked;

        private ParkedQuery(
            String coordinator,
            ActionFuture<EsqlQueryResponse> query,
            long docs,
            long idSum,
            AtomicInteger dataRequestsOnOldNode,
            AtomicInteger dataRequestsOnNewNode,
            BlockClusterStateProcessing blocked
        ) {
            this.coordinator = coordinator;
            this.query = query;
            this.docs = docs;
            this.idSum = idSum;
            this.dataRequestsOnOldNode = dataRequestsOnOldNode;
            this.dataRequestsOnNewNode = dataRequestsOnNewNode;
            this.blocked = blocked;
        }

        /** Lets the coordinator apply the states it missed. Idempotent, so it can also run unconditionally in {@code finally}. */
        void unblockCoordinator() {
            if (blocked != null) {
                blocked.stopDisrupting();
                internalCluster().clearDisruptionScheme();
                blocked = null;
            }
        }
    }

    /**
     * Starts a query and brings it to the point where its coordinator waits for newer routing:
     * <ol>
     * <li>The index has one shard and no replica, on the old node. The first open of an exchange on that node is held, without
     * blocking a thread. Once it is held the coordinator is past field caps and its first shard resolution, on current routing.</li>
     * <li>The coordinator stops applying cluster states, and the shard is moved to the new node. The old node drops its copy.</li>
     * <li>The open is released. The coordinator sends the data node request to the old node, which answers for a shard it no
     * longer has: the real answer, not a simulated one. The coordinator resolves again on its stale state, finds only the copy it
     * has tried, and waits.</li>
     * </ol>
     */
    private ParkedQuery parkQueryOnStaleRouting(boolean allowPartialResults) throws Exception {
        String master = internalCluster().startMasterOnlyNode();
        String coordinator = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        String oldNode = internalCluster().startDataOnlyNode();
        String newNode = internalCluster().startDataOnlyNode();
        ensureStableCluster(4);

        assertAcked(
            client(master).admin()
                .indices()
                .prepareCreate(INDEX)
                .setSettings(indexSettings(1, 0).put("index.routing.allocation.include._name", oldNode))
                .setMapping("id", "type=long")
        );
        long docs = between(10, 100);
        var bulk = client(master).prepareBulk(INDEX).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (long id = 0; id < docs; id++) {
            bulk.add(new IndexRequest().source("id", id));
        }
        assertFalse(bulk.get().hasFailures());
        ensureGreen(INDEX);
        Index index = resolveIndex(INDEX);

        AtomicInteger dataRequestsOnOldNode = countDataRequests(oldNode);
        AtomicInteger dataRequestsOnNewNode = countDataRequests(newNode);

        CountDownLatch openHeld = new CountDownLatch(1);
        SubscribableListener<Void> releaseOpen = new SubscribableListener<>();
        AtomicBoolean firstOpen = new AtomicBoolean(true);
        MockTransportService.getInstance(oldNode)
            .addRequestHandlingBehavior(ExchangeService.OPEN_EXCHANGE_ACTION_NAME, (handler, request, channel, task) -> {
                if (firstOpen.compareAndSet(true, false) == false) {
                    handler.messageReceived(request, channel, task);
                    return;
                }
                request.mustIncRef();
                releaseOpen.addListener(ActionListener.running(() -> {
                    try {
                        handler.messageReceived(request, channel, task);
                    } catch (Exception e) {
                        channel.sendResponse(e);
                    } finally {
                        request.decRef();
                    }
                }));
                openHeld.countDown();
            });

        ParkedQuery parked = null;
        try {
            ActionFuture<EsqlQueryResponse> query = client(coordinator).execute(
                EsqlQueryAction.INSTANCE,
                syncEsqlQueryRequest(QUERY).allowPartialResults(allowPartialResults)
            );
            safeAwait(openHeld);

            BlockClusterStateProcessing blocked = new BlockClusterStateProcessing(coordinator, random());
            parked = new ParkedQuery(
                coordinator,
                query,
                docs,
                docs * (docs - 1) / 2,
                dataRequestsOnOldNode,
                dataRequestsOnNewNode,
                blocked
            );
            internalCluster().setDisruptionScheme(blocked);
            blocked.startDisrupting();
            try (var mockLog = MockLog.capture(SENDER_LOGGER)) {
                // the coordinator cannot acknowledge, so the update is not waited for; the state of the master is
                client(master).admin()
                    .indices()
                    .prepareUpdateSettings(INDEX)
                    .setSettings(Settings.builder().put("index.routing.allocation.include._name", newNode))
                    .setTimeout(TimeValue.ZERO)
                    .get();
                String newNodeId = getNodeId(newNode);
                awaitClusterState(master, state -> {
                    ShardRouting primary = state.routingTable(ProjectId.DEFAULT).shardRoutingTable(INDEX, 0).primaryShard();
                    return primary.started() && newNodeId.equals(primary.currentNodeId());
                });
                IndicesService oldNodeIndices = internalCluster().getInstance(IndicesService.class, oldNode);
                assertBusy(() -> assertNull("the old node has dropped its copy", oldNodeIndices.indexService(index)));

                mockLog.addExpectation(
                    new MockLog.SeenEventExpectation(
                        "routing wait on the coordinator",
                        SENDER_LOGGER,
                        Level.DEBUG,
                        "waiting for newer shard routing for [1] shards for up to [*] from cluster state version [*]"
                    )
                );
                releaseOpen.onResponse(null);
                mockLog.awaitAllExpectationsMatched();
                assertFalse("the query waits for the routing", query.isDone());
                return parked;
            }
        } catch (Exception | AssertionError e) {
            if (releaseOpen.isDone() == false) {
                releaseOpen.onResponse(null);
            }
            if (parked != null) {
                parked.unblockCoordinator();
            }
            throw e;
        }
    }

    /** Counts the data node requests that reach the handler of the node. */
    private static AtomicInteger countDataRequests(String nodeName) {
        AtomicInteger requests = new AtomicInteger();
        MockTransportService.getInstance(nodeName)
            .addRequestHandlingBehavior(ComputeService.DATA_ACTION_NAME, (handler, request, channel, task) -> {
                requests.incrementAndGet();
                handler.messageReceived(request, channel, task);
            });
        return requests;
    }
}
