/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.InternalSnapshotsInfoService;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.elasticsearch.test.gateway.TestGatewayAllocator;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * Documents that a restore must survive a temporary absence of data nodes.
 * <p>
 * With no data nodes, allocation correctly cannot place restore primaries. That wait must not
 * permanently fail the restore: once a data node returns, allocation should proceed. Today
 * {@code failAllocationOfNewPrimaries} synthesizes {@link org.elasticsearch.cluster.routing.UnassignedInfo.AllocationStatus#DECIDERS_NO}
 * so cluster health turns red, {@link org.elasticsearch.snapshots.RestoreService.RestoreInProgressUpdater}
 * treats that as a terminal restore failure, and
 * {@link org.elasticsearch.cluster.routing.allocation.decider.RestoreInProgressAllocationDecider}
 * then blocks the shard forever — forcing a manual retry.
 */
public class RestoreAfterNoDataNodesAllocationTests extends ESAllocationTestCase {

    public void testRestoreContinuesAfterTemporaryAbsenceOfDataNodes() {
        final Snapshot snapshot = new Snapshot("repository", new SnapshotId("snapshot", UUIDs.randomBase64UUID(random())));
        final IndexId indexId = new IndexId("test", UUIDs.randomBase64UUID(random()));
        final String restoreUUID = UUIDs.randomBase64UUID(random());
        final RecoverySource.SnapshotRecoverySource recoverySource = new RecoverySource.SnapshotRecoverySource(
            restoreUUID,
            snapshot,
            IndexVersion.current(),
            indexId
        );

        // Snapshotted indices carry their in-sync allocation IDs into the restored IndexMetadata.
        final IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putInSyncAllocationIds(0, Set.of(UUIDs.randomBase64UUID(random())))
            .build();
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);

        final DiscoveryNode masterNode = newNode("master", Set.of(DiscoveryNodeRole.MASTER_ROLE));
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(masterNode).localNodeId(masterNode.getId()).masterNodeId(masterNode.getId()))
            .metadata(Metadata.builder().put(indexMetadata, false))
            .routingTable(
                RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
                    .addAsNewRestore(indexMetadata, recoverySource, Set.of())
            )
            .putCustom(
                RestoreInProgress.TYPE,
                new RestoreInProgress.Builder().add(
                    new RestoreInProgress.Entry(
                        restoreUUID,
                        snapshot,
                        RestoreInProgress.State.STARTED,
                        false,
                        List.of(indexMetadata.getIndex().getName()),
                        Map.of(shardId, new RestoreInProgress.ShardRestoreStatus(masterNode.getId()))
                    )
                ).build()
            )
            .build();

        final long shardSize = randomNonNegativeLong();
        final AllocationService allocationService = createAllocationService(
            Settings.EMPTY,
            new TestGatewayAllocator(),
            EmptyClusterInfoService.INSTANCE,
            () -> new SnapshotShardSizeInfo(
                Map.of(new InternalSnapshotsInfoService.SnapshotShard(snapshot, indexId, shardId), shardSize)
            )
        );

        // Allocation with zero data nodes: shards stay unassigned, but the restore must remain in progress.
        clusterState = allocationService.reroute(clusterState, "reroute with no data nodes", ActionListener.noop());
        ShardRouting primary = clusterState.routingTable().index("test").shard(0).primaryShard();
        assertThat(primary.state(), equalTo(ShardRoutingState.UNASSIGNED));
        assertThat(primary.recoverySource().getType(), equalTo(RecoverySource.Type.SNAPSHOT));

        RestoreInProgress.Entry restoreAfterNoDataNodes = RestoreInProgress.get(clusterState).get(restoreUUID);
        assertNotNull(restoreAfterNoDataNodes);
        assertThat(
            "temporary absence of data nodes must not permanently fail the restore",
            restoreAfterNoDataNodes.shards().get(shardId).state(),
            not(equalTo(RestoreInProgress.State.FAILURE))
        );
        assertFalse(
            "restore must remain in progress while waiting for data nodes",
            restoreAfterNoDataNodes.state().completed()
        );

        // Data node returns: restore should be able to allocate the primary.
        final DiscoveryNode dataNode = newNode("data", Set.of(DiscoveryNodeRole.DATA_ROLE));
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).add(dataNode))
            .build();
        clusterState = allocationService.reroute(clusterState, "reroute after data node joins", ActionListener.noop());

        primary = clusterState.routingTable().index("test").shard(0).primaryShard();
        assertThat(
            "restore primary must allocate once a data node is available again",
            primary.state(),
            equalTo(ShardRoutingState.INITIALIZING)
        );
        assertThat(primary.currentNodeId(), equalTo(dataNode.getId()));
    }
}
