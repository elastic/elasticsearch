/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.allocation.DesiredBalanceRequest;
import org.elasticsearch.action.admin.cluster.allocation.TransportGetDesiredBalanceAction;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.shard.FailedShardEntry;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.RecoveryDirectCancellationService;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportResponse;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class MasterTriggeredDirectCancellationIT extends AbstractIndexRecoveryIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final var plugins = new ArrayList<>(super.nodePlugins());
        plugins.addAll(List.of(TestTelemetryPlugin.class, TestRecoveryBlockerPlugin.class));
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(RecoveryDirectCancellationService.ENABLE_DIRECT_RECOVERY_CANCELLATIONS_SETTING.getKey(), true)
            .put(ClusterModule.SHARDS_ALLOCATOR_TYPE_SETTING.getKey(), ClusterModule.DESIRED_BALANCE_ALLOCATOR)
            .put(ThrottlingRecoveryService.INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING.getKey(), 1)
            .put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), "none")
            .build();
    }

    @Before
    public void resetPluginGates() {
        TestRecoveryBlockerPlugin.reset();
    }

    @After
    public void verifyNoOutstandingRecoveriesInStatsAndMetrics() {
        final var nodes = internalCluster().getNodeNames();
        awaitNoCurrentRecoveriesInStats(Arrays.asList(nodes));

        final var nodeToTelemetry = Arrays.stream(nodes)
            .collect(
                Collectors.toMap(
                    node -> node,
                    node -> internalCluster().getInstance(PluginsService.class, node)
                        .filterPlugins(TestTelemetryPlugin.class)
                        .findFirst()
                        .orElseThrow()
                )
            );
        final var noMoreRecoveriesInMetrics = Map.of(
            RecoveryMetricsCollector.QUEUED_STORE_RECOVERIES,
            0L,
            RecoveryMetricsCollector.CURRENT_STORE_RECOVERIES,
            0L,
            RecoveryMetricsCollector.QUEUED_PEER_RECOVERIES_AS_SOURCE,
            0L,
            RecoveryMetricsCollector.CURRENT_PEER_RECOVERIES_AS_SOURCE,
            0L,
            RecoveryMetricsCollector.QUEUED_PEER_RECOVERIES_AS_TARGET,
            0L,
            RecoveryMetricsCollector.CURRENT_PEER_RECOVERIES_AS_TARGET,
            0L
        );
        awaitRecoveryCountMetrics(
            nodeToTelemetry,
            Arrays.stream(nodes).collect(Collectors.toMap(node -> node, ignored -> noMoreRecoveriesInMetrics))
        );
    }

    public void testDesiredBalanceChangeCancelsQueuedReplicaRecovery() throws Exception {
        final var primaryNode = internalCluster().startNode();
        final var initialReplicaNode = internalCluster().startDataOnlyNode();
        final var desiredReplicaNode = internalCluster().startDataOnlyNode();
        final var indexName = randomIndexName();
        final var blockingIndex = randomIndexName();

        createIndex(indexName, indexSettings(1, 0).put("index.routing.allocation.include._name", primaryNode).build());
        ensureGreen(indexName);

        safeAcquire(TestRecoveryBlockerPlugin.beforeRecoveryGate);
        // Takes the only recovery slot on initialReplicaNode
        assertAcked(
            prepareCreate(blockingIndex).setSettings(indexSettings(1, 0).put("index.routing.allocation.include._name", initialReplicaNode))
                .setWaitForActiveShards(ActiveShardCount.NONE)
        );
        safeAcquire(TestRecoveryBlockerPlugin.beforeRecoveryEntered);
        TestRecoveryBlockerPlugin.beforeRecoveryEntered.release();

        updateSettings(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put("index.routing.allocation.include._name", primaryNode + "," + initialReplicaNode)
        );
        awaitRecoveryCountStats(Map.of(initialReplicaNode, stats -> stats.currentFromStore() == 1 && stats.currentAsTargetQueued() == 1));
        waitNoPendingTasksOnAll();

        final var shardId = new ShardId(resolveIndex(indexName), 0);
        final var expectedCancellation = new ShardRecoveryCancellation(
            shardId,
            getInitializingAllocationId(indexName, 0, getNodeId(initialReplicaNode)),
            true
        );
        final var requestAndResponse = assertDirectCancellationExchange(
            initialReplicaNode,
            clusterService().state(),
            List.of(expectedCancellation),
            Set.of(new CancelRecoveriesAction.CancelledInQueue(expectedCancellation.shardId(), expectedCancellation.allocationId()))
        );

        // Switch the desired node to desiredReplicaNode
        updateSettings(indexName, Settings.builder().put("index.routing.allocation.include._name", primaryNode + "," + desiredReplicaNode));

        awaitDesiredBalanceShardAssignment(indexName, 0, Set.of(getNodeId(primaryNode), getNodeId(desiredReplicaNode)));
        safeAwait(requestAndResponse);

        TestRecoveryBlockerPlugin.beforeRecoveryGate.release();
        ensureGreen(indexName, blockingIndex);
        awaitReplicaReassigned(indexName, 0, getNodeId(desiredReplicaNode));
    }

    public void testDesiredBalanceChangeCancelsStartedReplicaRecovery() throws Exception {
        final var primaryNode = internalCluster().startNode();
        final var initialReplicaNode = internalCluster().startDataOnlyNode();
        final var desiredReplicaNode = internalCluster().startDataOnlyNode();
        final var indexName = randomIndexName();

        createIndex(indexName, indexSettings(1, 0).put("index.routing.allocation.include._name", primaryNode).build());

        for (int i = 0; i < 50; i++) {
            indexDoc(indexName, Integer.toString(i), "f", randomAlphaOfLength(10));
            refresh(indexName);
        }
        flush(indexName);
        ensureGreen(indexName);

        final var blockedRecovery = new CountDownLatch(1);
        final var proceedWithRecovery = new CountDownLatch(1);
        final var blockedOnce = new AtomicBoolean();

        MockTransportService.getInstance(primaryNode).addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(PeerRecoveryTargetService.Actions.FILE_CHUNK) && blockedOnce.compareAndSet(false, true)) {
                blockedRecovery.countDown();
                safeAwait(proceedWithRecovery);
            }
            connection.sendRequest(requestId, action, request, options);
        });

        updateSettings(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put("index.routing.allocation.include._name", primaryNode + "," + initialReplicaNode)
        );

        safeAwait(blockedRecovery);
        waitNoPendingTasksOnAll();

        final var shardId = new ShardId(resolveIndex(indexName), 0);
        final var allocationId = getInitializingAllocationId(indexName, 0, getNodeId(initialReplicaNode));
        final var expectedCancellation = new ShardRecoveryCancellation(shardId, allocationId, true);
        final var requestAndResponse = assertDirectCancellationExchange(
            initialReplicaNode,
            clusterService().state(),
            List.of(expectedCancellation),
            Set.of()
        );
        final var shardFailureReceived = shardCancelledFailureReceivedLatch(internalCluster().getMasterName(), shardId);

        updateSettings(indexName, Settings.builder().put("index.routing.allocation.include._name", primaryNode + "," + desiredReplicaNode));

        awaitDesiredBalanceShardAssignment(indexName, 0, Set.of(getNodeId(primaryNode), getNodeId(desiredReplicaNode)));
        safeAwait(requestAndResponse);

        proceedWithRecovery.countDown();
        safeAwait(shardFailureReceived);
        ensureGreen(indexName);

        awaitDirectCancellationMetric(initialReplicaNode, 1L);
        awaitReplicaReassigned(indexName, 0, getNodeId(desiredReplicaNode));
    }

    public void testDesiredBalanceChangeCancelsQueuedStoreRecovery() throws Exception {
        final var initialPrimaryNode = internalCluster().startNode();
        final var desiredPrimaryNode = internalCluster().startDataOnlyNode();
        final var indexName = randomIndexName();
        final var blockingIndex = randomIndexName();

        safeAcquire(TestRecoveryBlockerPlugin.beforeRecoveryGate);
        // Takes the only recovery slot on initialPrimaryNode
        assertAcked(
            prepareCreate(blockingIndex).setSettings(indexSettings(1, 0).put("index.routing.allocation.include._name", initialPrimaryNode))
                .setWaitForActiveShards(ActiveShardCount.NONE)
        );
        safeAcquire(TestRecoveryBlockerPlugin.beforeRecoveryEntered);
        TestRecoveryBlockerPlugin.beforeRecoveryEntered.release();

        assertAcked(
            prepareCreate(indexName).setSettings(indexSettings(1, 0).put("index.routing.allocation.include._name", initialPrimaryNode))
                .setWaitForActiveShards(ActiveShardCount.NONE)
        );
        awaitRecoveryCountStats(Map.of(initialPrimaryNode, stats -> stats.currentFromStore() == 1 && stats.currentFromStoreQueued() == 1));
        waitNoPendingTasksOnAll();

        final var shardId = new ShardId(resolveIndex(indexName), 0);
        final var expectedCancellation = new ShardRecoveryCancellation(
            shardId,
            getInitializingAllocationId(indexName, 0, getNodeId(initialPrimaryNode)),
            false
        );
        final var requestAndResponse = assertDirectCancellationExchange(
            initialPrimaryNode,
            clusterService().state(),
            List.of(expectedCancellation),
            Set.of(new CancelRecoveriesAction.CancelledInQueue(expectedCancellation.shardId(), expectedCancellation.allocationId()))
        );

        updateSettings(indexName, Settings.builder().put("index.routing.allocation.include._name", desiredPrimaryNode));

        awaitDesiredBalanceShardAssignment(indexName, 0, Set.of(getNodeId(desiredPrimaryNode)));
        safeAwait(requestAndResponse);

        TestRecoveryBlockerPlugin.beforeRecoveryGate.release();
        ensureGreen(indexName, blockingIndex);
        awaitPrimaryReassigned(indexName, 0, getNodeId(desiredPrimaryNode));
    }

    public void testDesiredBalanceChangeCancelsStartedPrimaryRelocation() throws Exception {
        final var sourceNode = internalCluster().startNode();
        final var initialTargetNode = internalCluster().startDataOnlyNode();
        final var indexName = randomIndexName();

        createIndex(indexName, indexSettings(1, 0).put("index.routing.allocation.include._name", sourceNode).build());

        for (int i = 0; i < 50; i++) {
            indexDoc(indexName, Integer.toString(i), "f", randomAlphaOfLength(10));
            refresh(indexName);
        }
        flush(indexName);
        ensureGreen(indexName);
        updateSettings(indexName, Settings.builder().put("index.routing.allocation.include._name", sourceNode + "," + initialTargetNode));

        final var blockedRelocation = new CountDownLatch(1);
        final var proceedWithRelocation = new CountDownLatch(1);
        final var blockedOnce = new AtomicBoolean();

        final var sourceTransportService = MockTransportService.getInstance(sourceNode);
        sourceTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(PeerRecoveryTargetService.Actions.FILE_CHUNK) && blockedOnce.compareAndSet(false, true)) {
                blockedRelocation.countDown();
                safeAwait(proceedWithRelocation);
            }
            connection.sendRequest(requestId, action, request, options);
        });

        ClusterRerouteUtils.reroute(client(), new MoveAllocationCommand(indexName, 0, sourceNode, initialTargetNode));
        safeAwait(blockedRelocation);

        final var shardId = new ShardId(resolveIndex(indexName), 0);
        final var allocationId = getInitializingAllocationId(indexName, 0, getNodeId(initialTargetNode));
        final var expectedCancellation = new ShardRecoveryCancellation(shardId, allocationId, true);
        final var requestAndResponse = assertDirectCancellationExchange(
            initialTargetNode,
            clusterService().state(),
            List.of(expectedCancellation),
            Set.of()
        );
        final var shardFailureReceived = shardCancelledFailureReceivedLatch(sourceTransportService, shardId);

        updateSettings(indexName, Settings.builder().put("index.routing.allocation.include._name", sourceNode));

        awaitDesiredBalanceShardAssignment(indexName, 0, Set.of(getNodeId(sourceNode)));
        safeAwait(requestAndResponse);

        proceedWithRelocation.countDown();
        safeAwait(shardFailureReceived);
        ensureGreen(indexName);

        awaitDirectCancellationMetric(initialTargetNode, 1L);
    }

    public void testDesiredBalanceChangeCancelsBatchOfRecoveriesOnSameNode() throws Exception {
        final var primaryNode = internalCluster().startNode();
        final var initialReplicaNode = internalCluster().startDataOnlyNode();
        final var desiredReplicaNode = internalCluster().startDataOnlyNode();
        final var startedIndex = randomIndexName();
        final var queuedIndex = randomIndexName();

        createIndex(startedIndex, indexSettings(1, 0).put("index.routing.allocation.include._name", primaryNode).build());
        for (int i = 0; i < 50; i++) {
            indexDoc(startedIndex, Integer.toString(i), "f", randomAlphaOfLength(10));
            refresh(startedIndex);
        }
        flush(startedIndex);

        createIndex(queuedIndex, indexSettings(1, 0).put("index.routing.allocation.include._name", primaryNode).build());
        ensureGreen(startedIndex, queuedIndex);

        final var blockedRecovery = new CountDownLatch(1);
        final var proceedWithRecovery = new CountDownLatch(1);
        final var blockedOnce = new AtomicBoolean();
        MockTransportService.getInstance(primaryNode).addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(PeerRecoveryTargetService.Actions.FILE_CHUNK) && blockedOnce.compareAndSet(false, true)) {
                blockedRecovery.countDown();
                safeAwait(proceedWithRecovery);
            }
            connection.sendRequest(requestId, action, request, options);
        });
        assertAcked(
            indicesAdmin().prepareUpdateSettings(startedIndex, queuedIndex)
                .setSettings(Settings.builder().put("index.routing.allocation.include._name", primaryNode + "," + initialReplicaNode))
        );
        waitNoPendingTasksOnAll();

        // Takes the only recovery slot on initialReplicaNode
        updateSettings(startedIndex, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1));
        waitNoPendingTasksOnAll();
        safeAwait(blockedRecovery);

        updateSettings(queuedIndex, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1));
        awaitRecoveryCountStats(Map.of(initialReplicaNode, stats -> stats.currentAsTarget() == 1 && stats.currentAsTargetQueued() == 1));
        waitNoPendingTasksOnAll();

        final var startedShardId = new ShardId(resolveIndex(startedIndex), 0);
        final var startedAllocationId = getInitializingAllocationId(startedIndex, 0, getNodeId(initialReplicaNode));
        final var queuedShardId = new ShardId(resolveIndex(queuedIndex), 0);
        final var queuedAllocationId = getInitializingAllocationId(queuedIndex, 0, getNodeId(initialReplicaNode));

        final var expectedCancellations = List.of(
            new ShardRecoveryCancellation(startedShardId, startedAllocationId, true),
            new ShardRecoveryCancellation(queuedShardId, queuedAllocationId, true)
        );
        final var expectedCancelledInQueue = Set.of(new CancelRecoveriesAction.CancelledInQueue(queuedShardId, queuedAllocationId));
        final var requestAndResponse = assertDirectCancellationExchange(
            initialReplicaNode,
            clusterService().state(),
            expectedCancellations,
            expectedCancelledInQueue
        );
        final var shardFailureReceived = shardCancelledFailureReceivedLatch(internalCluster().getMasterName(), startedShardId);

        assertAcked(
            indicesAdmin().prepareUpdateSettings(startedIndex, queuedIndex)
                .setSettings(Settings.builder().put("index.routing.allocation.include._name", primaryNode + "," + desiredReplicaNode))
        );

        safeAwait(requestAndResponse);
        proceedWithRecovery.countDown();
        safeAwait(shardFailureReceived);
        ensureGreen(startedIndex, queuedIndex);

        awaitReplicaReassigned(startedIndex, 0, getNodeId(desiredReplicaNode));
        awaitReplicaReassigned(queuedIndex, 0, getNodeId(desiredReplicaNode));
    }

    public void testDesiredBalanceChangeCancelsRecoveriesOnDistinctNodes() throws Exception {
        final var primaryNode = internalCluster().startNode();
        final var nodeA = internalCluster().startDataOnlyNode();
        final var nodeB = internalCluster().startDataOnlyNode();
        final var desiredNode = internalCluster().startDataOnlyNode();
        final var indexA = randomIndexName();
        final var indexB = randomIndexName();
        final var blockingIndexA = randomIndexName();
        final var blockingIndexB = randomIndexName();

        createIndex(indexA, indexSettings(1, 0).put("index.routing.allocation.include._name", primaryNode).build());
        createIndex(indexB, indexSettings(1, 0).put("index.routing.allocation.include._name", primaryNode).build());
        ensureGreen(indexA, indexB);

        safeAcquire(TestRecoveryBlockerPlugin.beforeRecoveryGate);

        assertAcked(
            prepareCreate(blockingIndexA).setSettings(indexSettings(1, 0).put("index.routing.allocation.include._name", nodeA))
                .setWaitForActiveShards(ActiveShardCount.NONE)
        );
        safeAcquire(TestRecoveryBlockerPlugin.beforeRecoveryEntered);
        TestRecoveryBlockerPlugin.beforeRecoveryEntered.release();

        assertAcked(
            prepareCreate(blockingIndexB).setSettings(indexSettings(1, 0).put("index.routing.allocation.include._name", nodeB))
                .setWaitForActiveShards(ActiveShardCount.NONE)
        );
        safeAcquire(TestRecoveryBlockerPlugin.beforeRecoveryEntered);
        TestRecoveryBlockerPlugin.beforeRecoveryEntered.release();

        updateSettings(
            indexA,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put("index.routing.allocation.include._name", primaryNode + "," + nodeA)
        );
        updateSettings(
            indexB,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put("index.routing.allocation.include._name", primaryNode + "," + nodeB)
        );
        awaitRecoveryCountStats(
            Map.of(
                nodeA,
                stats -> stats.currentFromStore() == 1 && stats.currentAsTargetQueued() == 1,
                nodeB,
                stats -> stats.currentFromStore() == 1 && stats.currentAsTargetQueued() == 1
            )
        );
        waitNoPendingTasksOnAll();

        final var shardIdA = new ShardId(resolveIndex(indexA), 0);
        final var allocationIdA = getInitializingAllocationId(indexA, 0, getNodeId(nodeA));
        final var shardIdB = new ShardId(resolveIndex(indexB), 0);
        final var allocationIdB = getInitializingAllocationId(indexB, 0, getNodeId(nodeB));

        final var expectedCancellationA = new ShardRecoveryCancellation(shardIdA, allocationIdA, true);
        final var expectedCancellationB = new ShardRecoveryCancellation(shardIdB, allocationIdB, true);

        final var requestAndResponseA = assertDirectCancellationExchange(
            nodeA,
            clusterService().state(),
            List.of(expectedCancellationA),
            Set.of(new CancelRecoveriesAction.CancelledInQueue(shardIdA, allocationIdA))
        );
        final var requestAndResponseB = assertDirectCancellationExchange(
            nodeB,
            clusterService().state(),
            List.of(expectedCancellationB),
            Set.of(new CancelRecoveriesAction.CancelledInQueue(shardIdB, allocationIdB))
        );

        assertAcked(
            indicesAdmin().prepareUpdateSettings(indexA, indexB)
                .setSettings(Settings.builder().put("index.routing.allocation.include._name", primaryNode + "," + desiredNode))
        );

        safeAwait(requestAndResponseA);
        safeAwait(requestAndResponseB);

        TestRecoveryBlockerPlugin.beforeRecoveryGate.release();
        ensureGreen(indexA, indexB, blockingIndexA, blockingIndexB);
        awaitReplicaReassigned(indexA, 0, getNodeId(desiredNode));
        awaitReplicaReassigned(indexB, 0, getNodeId(desiredNode));
    }

    private CountDownLatch assertDirectCancellationExchange(
        String node,
        ClusterState state,
        List<ShardRecoveryCancellation> expectedCancellations,
        Set<CancelRecoveriesAction.CancelledInQueue> expectedCancelledInQueue
    ) {
        final CountDownLatch requestAndResponse = new CountDownLatch(2);
        MockTransportService.getInstance(node)
            .addRequestHandlingBehavior(CancelRecoveriesAction.TYPE.name(), (handler, request, channel, task) -> {
                if (request instanceof CancelRecoveriesAction.Request cancelRequest) {
                    assertExpectedRequest(cancelRequest, state, expectedCancellations);
                    requestAndResponse.countDown();
                    handler.messageReceived(request, new TransportChannel() {
                        @Override
                        public String getProfileName() {
                            return channel.getProfileName();
                        }

                        @Override
                        public void sendResponse(TransportResponse response) {
                            assertTrue(response instanceof CancelRecoveriesAction.Response);
                            assertExpectedResponse((CancelRecoveriesAction.Response) response, expectedCancelledInQueue);
                            requestAndResponse.countDown();
                            channel.sendResponse(response);
                        }

                        @Override
                        public void sendResponse(Exception exception) {
                            channel.sendResponse(exception);
                        }
                    }, task);
                    return;
                }
                handler.messageReceived(request, channel, task);
            });
        return requestAndResponse;
    }

    private void assertExpectedRequest(
        CancelRecoveriesAction.Request request,
        ClusterState state,
        List<ShardRecoveryCancellation> expectedCancellations
    ) {
        assertThat(request.term(), greaterThanOrEqualTo(state.term()));
        assertThat(request.clusterStateVersion(), greaterThanOrEqualTo(state.version()));
        assertThat(request.cancellations().size(), equalTo(expectedCancellations.size()));
        assertThat(
            request.cancellations(),
            containsInAnyOrder(expectedCancellations.toArray(new ShardRecoveryCancellation[expectedCancellations.size()]))
        );
    }

    private void assertExpectedResponse(
        CancelRecoveriesAction.Response response,
        Set<CancelRecoveriesAction.CancelledInQueue> expectedCancelledInQueue
    ) {
        assertThat(response.cancelledInQueue(), equalTo(expectedCancelledInQueue));
    }

    private void updateSettings(String indexName, Settings.Builder settings) {
        assertAcked(indicesAdmin().prepareUpdateSettings(indexName).setSettings(settings));
    }

    private String getInitializingAllocationId(String indexName, int shardId, String expectedNodeId) {
        final var allocationId = new AtomicReference<String>();
        awaitClusterState(state -> {
            final var shardRoutingTable = state.routingTable().index(indexName).shard(shardId);
            final var initializingShards = shardRoutingTable.shardsWithState(ShardRoutingState.INITIALIZING);
            if (initializingShards.size() != 1) {
                return false;
            }
            final var initializingShard = initializingShards.getFirst();
            if (expectedNodeId.equals(initializingShard.currentNodeId()) == false) {
                return false;
            }
            allocationId.set(initializingShard.allocationId().getId());
            return true;
        });
        return allocationId.get();
    }

    private void awaitDesiredBalanceShardAssignment(String indexName, int shardId, Set<String> expectedNodeIds) throws Exception {
        assertBusy(() -> {
            final var desiredBalanceResponse = safeGet(
                client().execute(TransportGetDesiredBalanceAction.TYPE, new DesiredBalanceRequest(TEST_REQUEST_TIMEOUT))
            );
            final var shards = desiredBalanceResponse.getRoutingTable().get(indexName);
            assertNotNull(shards);
            final var desiredShards = shards.get(shardId);
            assertNotNull(desiredShards);
            assertThat(desiredShards.desired().nodeIds(), equalTo(expectedNodeIds));
        });
    }

    private void awaitReplicaReassigned(String indexName, int shardId, String expectedNodeId) {
        awaitClusterState(state -> {
            final var shardRoutingTable = state.routingTable().index(indexName).shard(shardId);
            final var replicas = shardRoutingTable.replicaShardsWithState(ShardRoutingState.STARTED);
            return replicas.size() == 1 && expectedNodeId.equals(replicas.getFirst().currentNodeId());
        });
    }

    private void awaitPrimaryReassigned(String indexName, int shardId, String expectedNodeId) throws Exception {
        awaitClusterState(state -> {
            final var primaryShard = state.routingTable().index(indexName).shard(shardId).primaryShard();
            return primaryShard.started() && expectedNodeId.equals(primaryShard.currentNodeId());
        });
    }

    private void awaitDirectCancellationMetric(String node, long value) {
        awaitRecoveryCountMetrics(
            node,
            internalCluster().getInstance(PluginsService.class, node).filterPlugins(TestTelemetryPlugin.class).findFirst().orElseThrow(),
            Map.of(RecoveryMetricsCollector.RECOVERY_DIRECT_CANCELLATIONS_METRIC, value)
        );
    }

    private static CountDownLatch shardCancelledFailureReceivedLatch(String node, ShardId shardId) {
        return shardCancelledFailureReceivedLatch(MockTransportService.getInstance(node), shardId);
    }

    private static CountDownLatch shardCancelledFailureReceivedLatch(MockTransportService transportService, ShardId shardId) {
        final var shardFailureReceivedLatch = new CountDownLatch(1);
        transportService.addRequestHandlingBehavior(ShardStateAction.SHARD_FAILED_ACTION_NAME, (handler, request, channel, task) -> {
            if (request instanceof FailedShardEntry failedShard) {
                if (failedShard.getShardId().equals(shardId)
                    && ExceptionsHelper.unwrap(failedShard.getFailure(), RecoveryCancelledException.class) != null) {
                    shardFailureReceivedLatch.countDown();
                }
            }
            handler.messageReceived(request, channel, task);
        });
        return shardFailureReceivedLatch;
    }

    public static class TestRecoveryBlockerPlugin extends Plugin {
        static final Semaphore beforeRecoveryGate = new Semaphore(1);
        static final Semaphore beforeRecoveryEntered = new Semaphore(0);

        static void reset() {
            beforeRecoveryGate.drainPermits();
            beforeRecoveryGate.release();
            beforeRecoveryEntered.drainPermits();
        }

        @Override
        public void onIndexModule(IndexModule indexModule) {
            indexModule.addIndexEventListener(new IndexEventListener() {
                @Override
                public void beforeIndexShardRecovery(IndexShard indexShard, IndexSettings indexSettings, ActionListener<Void> listener) {
                    beforeRecoveryEntered.release();
                    safeAcquire(beforeRecoveryGate);
                    beforeRecoveryGate.release();
                    safeAcquire(beforeRecoveryEntered);
                    listener.onResponse(null);
                }
            });
        }
    }

}
