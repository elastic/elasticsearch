/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gateway;

import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNodesHelper;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.index.seqno.RetentionLease;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING;
import static org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ReplicaShardAllocatorIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class, InternalSettingsPlugin.class);
    }

    /**
     * Verify that if we found a new copy where it can perform a no-op recovery,
     * then we will cancel the current recovery and allocate replica to the new copy.
     */
    public void testPreferCopyCanPerformNoopRecovery() throws Exception {
        String indexName = "test";
        String nodeWithPrimary = internalCluster().startNode();

        updateClusterSettings(
            Settings.builder().put(CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Rebalance.NONE)
        );

        assertAcked(
            indicesAdmin().prepareCreate(indexName)
                .setSettings(
                    indexSettings(1, 1).put(IndexSettings.FILE_BASED_RECOVERY_THRESHOLD_SETTING.getKey(), 1.0f)
                        .put(IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(), "100ms")
                        .put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), "100ms")
                        .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "1ms")
                )
        );
        String nodeWithReplica = internalCluster().startDataOnlyNode();
        Settings nodeWithReplicaSettings = internalCluster().dataPathSettings(nodeWithReplica);
        ensureGreen(indexName);
        indexRandom(
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            IntStream.range(0, between(100, 500)).mapToObj(n -> prepareIndex(indexName).setSource("f", "v")).toList()
        );
        indicesAdmin().prepareFlush(indexName).get();
        if (randomBoolean()) {
            indexRandom(
                randomBoolean(),
                false,
                randomBoolean(),
                IntStream.range(0, between(0, 80)).mapToObj(n -> prepareIndex(indexName).setSource("f", "v")).toList()
            );
        }
        ensureActivePeerRecoveryRetentionLeasesAdvanced(indexName);
        internalCluster().stopNode(nodeWithReplica);
        if (randomBoolean()) {
            indicesAdmin().prepareForceMerge(indexName).setFlush(true).get();
        }
        CountDownLatch blockRecovery = new CountDownLatch(1);
        CountDownLatch recoveryStarted = new CountDownLatch(1);
        final var transportServiceOnPrimary = MockTransportService.getInstance(nodeWithPrimary);
        transportServiceOnPrimary.addSendBehavior((connection, requestId, action, request, options) -> {
            if (PeerRecoveryTargetService.Actions.FILES_INFO.equals(action)) {
                recoveryStarted.countDown();
                safeAwait(blockRecovery);
            }
            connection.sendRequest(requestId, action, request, options);
        });
        internalCluster().startDataOnlyNode();
        safeAwait(recoveryStarted);
        nodeWithReplica = internalCluster().startDataOnlyNode(nodeWithReplicaSettings);
        // AllocationService only calls GatewayAllocator if there are unassigned shards
        assertAcked(indicesAdmin().prepareCreate("dummy-index").setWaitForActiveShards(0));
        ensureGreen(indexName);
        assertThat(internalCluster().nodesInclude(indexName), containsInAnyOrder(nodeWithPrimary, nodeWithReplica));
        assertNoOpRecoveries(indexName);
        blockRecovery.countDown();
        transportServiceOnPrimary.clearAllRules();
    }

    /**
     * Ensure that we fetch the latest shard store from the primary when a new node joins so we won't cancel the current recovery
     * for the copy on the newly joined node unless we can perform a noop recovery with that node.
     */
    public void testRecentPrimaryInformation() throws Exception {
        String indexName = "test";
        String nodeWithPrimary = internalCluster().startNode();
        assertAcked(
            indicesAdmin().prepareCreate(indexName)
                .setSettings(
                    indexSettings(1, 1).put(IndexSettings.FILE_BASED_RECOVERY_THRESHOLD_SETTING.getKey(), 0.1f)
                        .put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), "100ms")
                        .put(IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(), "100ms")
                        .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "1ms")
                )
        );
        String nodeWithReplica = internalCluster().startDataOnlyNode();
        DiscoveryNode discoNodeWithReplica = internalCluster().getInstance(ClusterService.class, nodeWithReplica).localNode();
        Settings nodeWithReplicaSettings = internalCluster().dataPathSettings(nodeWithReplica);
        ensureGreen(indexName);

        indexRandom(
            randomBoolean(),
            false,
            randomBoolean(),
            IntStream.range(0, between(10, 100)).mapToObj(n -> prepareIndex(indexName).setSource("f", "v")).toList()
        );
        internalCluster().stopNode(nodeWithReplica);
        if (randomBoolean()) {
            indexRandom(
                randomBoolean(),
                false,
                randomBoolean(),
                IntStream.range(0, between(10, 100)).mapToObj(n -> prepareIndex(indexName).setSource("f", "v")).toList()
            );
        }
        CountDownLatch blockRecovery = new CountDownLatch(1);
        CountDownLatch recoveryStarted = new CountDownLatch(1);
        final var transportServiceOnPrimary = MockTransportService.getInstance(nodeWithPrimary);
        transportServiceOnPrimary.addSendBehavior((connection, requestId, action, request, options) -> {
            if (PeerRecoveryTargetService.Actions.FILES_INFO.equals(action)) {
                recoveryStarted.countDown();
                try {
                    blockRecovery.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });
        String newNode = internalCluster().startDataOnlyNode();
        recoveryStarted.await();
        // Index more documents and flush to destroy sync_id and remove the retention lease (as file_based_recovery_threshold reached).
        indexRandom(
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            IntStream.range(0, between(50, 200)).mapToObj(n -> prepareIndex(indexName).setSource("f", "v")).toList()
        );
        indicesAdmin().prepareFlush(indexName).get();
        assertBusy(() -> {
            for (ShardStats shardStats : indicesAdmin().prepareStats(indexName).get().getShards()) {
                for (RetentionLease lease : shardStats.getRetentionLeaseStats().retentionLeases().leases()) {
                    assertThat(lease.id(), not(equalTo(ReplicationTracker.getPeerRecoveryRetentionLeaseId(discoNodeWithReplica.getId()))));
                }
            }
        });
        // AllocationService only calls GatewayAllocator if there are unassigned shards
        assertAcked(
            indicesAdmin().prepareCreate("dummy-index")
                .setWaitForActiveShards(0)
                .setSettings(Settings.builder().put("index.routing.allocation.require.attr", "not-found"))
        );
        internalCluster().startDataOnlyNode(nodeWithReplicaSettings);
        // need to wait for events to ensure the reroute has happened since we perform it async when a new node joins.
        clusterAdmin().prepareHealth(indexName).setWaitForYellowStatus().setWaitForEvents(Priority.LANGUID).get();
        blockRecovery.countDown();
        ensureGreen(indexName);
        assertThat(internalCluster().nodesInclude(indexName), hasItem(newNode));
        for (RecoveryState recovery : indicesAdmin().prepareRecoveries(indexName).get().shardRecoveryStates().get(indexName)) {
            if (recovery.getPrimary() == false) {
                assertThat(recovery.getIndex().fileDetails(), not(empty()));
            }
        }
        transportServiceOnPrimary.clearAllRules();
    }

    public void testFullClusterRestartPerformNoopRecovery() throws Exception {
        int numOfReplicas = randomIntBetween(1, 2);
        internalCluster().ensureAtLeastNumDataNodes(numOfReplicas + 2);
        String indexName = "test";
        assertAcked(
            indicesAdmin().prepareCreate(indexName)
                .setSettings(
                    indexSettings(1, numOfReplicas).put(
                        IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(),
                        randomIntBetween(10, 100) + "kb"
                    )
                        .put(IndexSettings.FILE_BASED_RECOVERY_THRESHOLD_SETTING.getKey(), 0.5)
                        .put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), "100ms")
                        .put(IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(), "100ms")
                )
        );
        ensureGreen(indexName);
        indexRandom(
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            IntStream.range(0, between(200, 500)).mapToObj(n -> prepareIndex(indexName).setSource("f", "v")).toList()
        );
        indicesAdmin().prepareFlush(indexName).get();
        indexRandom(
            randomBoolean(),
            false,
            randomBoolean(),
            IntStream.range(0, between(0, 80)).mapToObj(n -> prepareIndex(indexName).setSource("f", "v")).toList()
        );
        if (randomBoolean()) {
            indicesAdmin().prepareForceMerge(indexName).get();
        }
        ensureActivePeerRecoveryRetentionLeasesAdvanced(indexName);
        if (randomBoolean()) {
            assertAcked(indicesAdmin().prepareClose(indexName));
        }
        updateClusterSettings(Settings.builder().put("cluster.routing.allocation.enable", "primaries"));
        internalCluster().fullRestart();
        ensureYellow(indexName);
        updateClusterSettings(Settings.builder().putNull("cluster.routing.allocation.enable"));
        ensureGreen(indexName);
        assertNoOpRecoveries(indexName);
    }

    public void testPreferCopyWithHighestMatchingOperations() throws Exception {
        String indexName = "test";
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNodes(3);
        assertAcked(
            indicesAdmin().prepareCreate(indexName)
                .setSettings(
                    indexSettings(1, 1).put(
                        IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(),
                        randomIntBetween(10, 100) + "kb"
                    )
                        .put(IndexSettings.FILE_BASED_RECOVERY_THRESHOLD_SETTING.getKey(), 3.0)
                        .put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), "100ms")
                        .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "0ms")
                        .put(IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(), "100ms")
                )
        );
        ensureGreen(indexName);
        indexRandom(
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            IntStream.range(0, between(200, 500)).mapToObj(n -> prepareIndex(indexName).setSource("f", "v")).toList()
        );
        indicesAdmin().prepareFlush(indexName).get();
        String nodeWithLowerMatching = randomFrom(internalCluster().nodesInclude(indexName));
        Settings nodeWithLowerMatchingSettings = internalCluster().dataPathSettings(nodeWithLowerMatching);
        internalCluster().stopNode(nodeWithLowerMatching);
        ensureGreen(indexName);

        indexRandom(
            randomBoolean(),
            false,
            randomBoolean(),
            IntStream.range(0, between(1, 100)).mapToObj(n -> prepareIndex(indexName).setSource("f", "v")).toList()
        );
        ensureActivePeerRecoveryRetentionLeasesAdvanced(indexName);
        String nodeWithHigherMatching = randomFrom(internalCluster().nodesInclude(indexName));
        Settings nodeWithHigherMatchingSettings = internalCluster().dataPathSettings(nodeWithHigherMatching);
        internalCluster().stopNode(nodeWithHigherMatching);
        if (usually()) {
            indexRandom(
                randomBoolean(),
                false,
                randomBoolean(),
                IntStream.range(0, between(1, 100)).mapToObj(n -> prepareIndex(indexName).setSource("f", "v")).toList()
            );
        }

        updateClusterSettings(Settings.builder().put("cluster.routing.allocation.enable", "primaries"));
        nodeWithLowerMatching = internalCluster().startNode(nodeWithLowerMatchingSettings);
        nodeWithHigherMatching = internalCluster().startNode(nodeWithHigherMatchingSettings);
        updateClusterSettings(
            Settings.builder()
                .put(CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Rebalance.NONE)
                .putNull(CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey())
        );
        ensureGreen(indexName);
        assertThat(internalCluster().nodesInclude(indexName), allOf(hasItem(nodeWithHigherMatching), not(hasItem(nodeWithLowerMatching))));
    }

    /**
     * Make sure that we do not repeatedly cancel an ongoing recovery for a noop copy on a broken node.
     */
    public void testDoNotCancelRecoveryForBrokenNode() throws Exception {
        internalCluster().startMasterOnlyNode();
        String nodeWithPrimary = internalCluster().startDataOnlyNode();
        String indexName = "test";
        assertAcked(
            indicesAdmin().prepareCreate(indexName)
                .setSettings(
                    indexSettings(1, 0).put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), "100ms")
                        .put(IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(), "100ms")
                )
        );
        indexRandom(
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            IntStream.range(0, between(200, 500)).mapToObj(n -> prepareIndex(indexName).setSource("f", "v")).toList()
        );
        indicesAdmin().prepareFlush(indexName).get();
        String brokenNode = internalCluster().startDataOnlyNode();
        final var transportService = MockTransportService.getInstance(nodeWithPrimary);
        CountDownLatch newNodeStarted = new CountDownLatch(1);
        transportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(PeerRecoveryTargetService.Actions.TRANSLOG_OPS)) {
                if (brokenNode.equals(connection.getNode().getName())) {
                    safeAwait(newNodeStarted);
                    throw new CircuitBreakingException("not enough memory for indexing", 100, 50, CircuitBreaker.Durability.TRANSIENT);
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });
        setReplicaCount(1, indexName);
        internalCluster().startDataOnlyNode();
        newNodeStarted.countDown();

        var allocator = internalCluster().getInstance(ShardsAllocator.class);
        if (allocator instanceof BalancedShardsAllocator) {
            // BalancedShardsAllocator will try other node once retries are exhausted
            ensureGreen(indexName);
        } else if (allocator instanceof DesiredBalanceShardsAllocator) {
            // DesiredBalanceShardsAllocator will keep shard in the error state if it could not be allocated on the desired node
            ensureYellow(indexName);
        } else {
            fail("Unknown allocator used");
        }

        transportService.clearAllRules();
    }

    public void testPeerRecoveryForClosedIndices() throws Exception {
        String indexName = "peer_recovery_closed_indices";
        internalCluster().ensureAtLeastNumDataNodes(1);
        createIndex(
            indexName,
            indexSettings(1, 0).put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), "100ms")
                .put(IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(), "100ms")
                .build()
        );
        indexRandom(
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            IntStream.range(0, randomIntBetween(1, 100)).mapToObj(n -> prepareIndex(indexName).setSource("num", n)).toList()
        );
        ensureActivePeerRecoveryRetentionLeasesAdvanced(indexName);
        assertAcked(indicesAdmin().prepareClose(indexName));
        int numberOfReplicas = randomIntBetween(1, 2);
        internalCluster().ensureAtLeastNumDataNodes(2 + numberOfReplicas);
        setReplicaCount(numberOfReplicas, indexName);
        ensureGreen(indexName);
        ensureActivePeerRecoveryRetentionLeasesAdvanced(indexName);
        updateClusterSettings(
            Settings.builder()
                .put(CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), EnableAllocationDecider.Allocation.PRIMARIES)
                .put(CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Allocation.PRIMARIES)
        );
        internalCluster().fullRestart();
        ensureYellow(indexName);
        if (randomBoolean()) {
            assertAcked(indicesAdmin().prepareOpen(indexName));
            indicesAdmin().prepareForceMerge(indexName).get();
        }
        updateClusterSettings(Settings.builder().putNull(CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey()));
        ensureGreen(indexName);
        assertNoOpRecoveries(indexName);
    }

    public static void ensureActivePeerRecoveryRetentionLeasesAdvanced(String indexName) throws Exception {
        final ClusterService clusterService = internalCluster().clusterService();
        assertBusy(() -> {
            Index index = resolveIndex(indexName);
            Set<String> activeRetentionLeaseIds = RoutingNodesHelper.asStream(clusterService.state().routingTable().index(index).shard(0))
                .map(shardRouting -> ReplicationTracker.getPeerRecoveryRetentionLeaseId(shardRouting.currentNodeId()))
                .collect(Collectors.toSet());
            for (String node : internalCluster().nodesInclude(indexName)) {
                IndexService indexService = internalCluster().getInstance(IndicesService.class, node).indexService(index);
                if (indexService != null) {
                    for (IndexShard shard : indexService) {
                        assertThat(shard.getLastSyncedGlobalCheckpoint(), equalTo(shard.seqNoStats().getMaxSeqNo()));
                        Set<RetentionLease> activeRetentionLeases = shard.getPeerRecoveryRetentionLeases()
                            .stream()
                            .filter(lease -> activeRetentionLeaseIds.contains(lease.id()))
                            .collect(Collectors.toSet());
                        assertThat(activeRetentionLeases, hasSize(activeRetentionLeaseIds.size()));
                        for (RetentionLease lease : activeRetentionLeases) {
                            assertThat(lease.retainingSequenceNumber(), equalTo(shard.getLastSyncedGlobalCheckpoint() + 1));
                        }
                    }
                }
            }
        });
    }

    private void assertNoOpRecoveries(String indexName) {
        for (RecoveryState recovery : indicesAdmin().prepareRecoveries(indexName).get().shardRecoveryStates().get(indexName)) {
            if (recovery.getPrimary() == false) {
                assertThat(recovery.getIndex().fileDetails(), empty());
                assertThat(recovery.getTranslog().totalLocal(), equalTo(recovery.getTranslog().totalOperations()));
            }
        }
    }
}
