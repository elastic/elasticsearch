/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceReconciler;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Semaphore;

import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class MasterTriggeredRecoveryCancellationIT extends AbstractIndexRecoveryIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(PeerRecoveryBlockerPlugin.class);
        return plugins;
    }

    @Before
    public void resetPluginGates() {
        // So that a failed test cannot corrupt subsequent ones.
        PeerRecoveryBlockerPlugin.reset();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(DesiredBalanceReconciler.ENABLE_INITIALIZING_SHARD_CANCELLATION_SETTING.getKey(), true)
            .put(RecoveryDirectCancellationService.ENABLE_DIRECT_RECOVERY_CANCELLATIONS_SETTING.getKey(), true)
            .build();
    }

    public void testMasterCancelsUndesiredReplicaRecoveryInProgress() throws Exception {
        internalCluster().startMasterOnlyNode();
        final var primaryNode = internalCluster().startDataOnlyNode();

        final var indexName = randomIndexName();

        // Block the replica's peer recovery before it can start
        safeAcquire(PeerRecoveryBlockerPlugin.recoveryGate);

        // Allocate the primary alone first, restricted to primaryNode
        createIndex(
            indexName,
            indexSettings(1, 0).put(
                IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getConcreteSettingForNamespace("_name").getKey(),
                primaryNode
            ).build()
        );
        indexDoc(indexName, "1", "f", randomAlphaOfLength(10));
        flush(indexName);
        ensureGreen(indexName);

        final var oldReplicaNode = internalCluster().startDataOnlyNode();
        updateIndexSettings(
            Settings.builder()
                .put(
                    IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getConcreteSettingForNamespace("_name").getKey(),
                    primaryNode + "," + oldReplicaNode
                )
                .put("index.number_of_replicas", 1),
            indexName
        );

        // Wait for the replica's peer recovery to reach the recovery gate.
        safeAcquire(PeerRecoveryBlockerPlugin.recoveryEntered);
        PeerRecoveryBlockerPlugin.recoveryEntered.release();

        final var index = resolveIndex(indexName);
        final var shardId = new ShardId(index, 0);
        final var oldReplicaIndicesService = internalCluster().getInstance(IndicesService.class, oldReplicaNode);
        final var oldReplicaShard = oldReplicaIndicesService.indexServiceSafe(index).getShard(0);
        assertTrue(oldReplicaShard.routingEntry().initializing());

        // The replica is no longer allowed on oldReplicaNode, only on newReplicaNode.
        final var newReplicaNode = internalCluster().startDataOnlyNode();
        final var newReplicaNodeId = getNodeId(newReplicaNode);
        updateIndexSettings(
            Settings.builder()
                .put(
                    IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getConcreteSettingForNamespace("_name").getKey(),
                    primaryNode + "," + newReplicaNode
                ),
            indexName
        );

        // The master should remove the initializing shard from oldReplicaNode via the routing table on its own,
        // while the recovery is still blocked at our gate.
        awaitClusterState(
            state -> state.routingTable().shardRoutingTable(shardId).replicaShards().getFirst().currentNodeId().equals(newReplicaNodeId)
        );

        // Release the now-obsolete blocked recovery attempt on oldReplicaNode
        PeerRecoveryBlockerPlugin.recoveryGate.release();

        ensureGreen(indexName);
        final var state = client().admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        final var replicaShardRouting = state.routingTable().shardRoutingTable(shardId).replicaShards().getFirst();
        assertThat(replicaShardRouting.currentNodeId(), equalTo(state.nodes().resolveNode(newReplicaNode).getId()));
    }

    public static class PeerRecoveryBlockerPlugin extends Plugin {
        static final Semaphore recoveryGate = new Semaphore(1);
        static final Semaphore recoveryEntered = new Semaphore(0);

        static void reset() {
            recoveryGate.drainPermits();
            recoveryGate.release();
            recoveryEntered.drainPermits();
        }

        @Override
        public List<Setting<?>> getSettings() {
            return List.of(
                DesiredBalanceReconciler.ENABLE_INITIALIZING_SHARD_CANCELLATION_SETTING,
                RecoveryDirectCancellationService.ENABLE_DIRECT_RECOVERY_CANCELLATIONS_SETTING
            );
        }

        @Override
        public void onIndexModule(IndexModule indexModule) {
            indexModule.addIndexEventListener(new IndexEventListener() {
                @Override
                public void beforeIndexShardRecovery(IndexShard indexShard, IndexSettings indexSettings, ActionListener<Void> listener) {
                    final var recoverySource = indexShard.recoveryState() == null ? null : indexShard.recoveryState().getRecoverySource();
                    if (indexShard.routingEntry().primary()
                        || recoverySource == null
                        || recoverySource.getType() != RecoverySource.Type.PEER) {
                        listener.onResponse(null);
                        return;
                    }
                    recoveryEntered.release();
                    safeAcquire(recoveryGate);
                    recoveryGate.release();
                    safeAcquire(recoveryEntered);
                    listener.onResponse(null);
                }
            });
        }
    }
}
