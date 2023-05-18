/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNodesHelper;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.elasticsearch.cluster.routing.RoutingNodesHelper.shardsWithState;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

/**
 * Test {@link RestoreInProgressAllocationDecider}
 */
public class RestoreInProgressAllocationDeciderTests extends ESAllocationTestCase {

    public void testCanAllocatePrimary() {
        ClusterState clusterState = createInitialClusterState();
        ShardRouting shard;
        if (randomBoolean()) {
            shard = clusterState.getRoutingTable().shardRoutingTable("test", 0).primaryShard();
            assertEquals(RecoverySource.Type.EMPTY_STORE, shard.recoverySource().getType());
        } else {
            shard = clusterState.getRoutingTable().shardRoutingTable("test", 0).replicaShards().get(0);
            assertEquals(RecoverySource.Type.PEER, shard.recoverySource().getType());
        }

        final Decision decision = executeAllocation(clusterState, shard);
        assertEquals(Decision.Type.YES, decision.type());
        assertEquals("ignored as shard is not being recovered from a snapshot", decision.getExplanation());
    }

    public void testCannotAllocatePrimaryMissingInRestoreInProgress() {
        ClusterState clusterState = createInitialClusterState();
        RoutingTable routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY, clusterState.getRoutingTable())
            .addAsRestore(clusterState.getMetadata().index("test"), createSnapshotRecoverySource("_missing"))
            .build();

        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        ShardRouting primary = clusterState.getRoutingTable().shardRoutingTable("test", 0).primaryShard();
        assertEquals(ShardRoutingState.UNASSIGNED, primary.state());
        assertEquals(RecoverySource.Type.SNAPSHOT, primary.recoverySource().getType());

        final Decision decision = executeAllocation(clusterState, primary);
        assertEquals(Decision.Type.NO, decision.type());
        assertThat(
            decision.getExplanation(),
            equalTo(
                "shard has failed to be restored from the snapshot [_repository:_missing/_uuid] - manually close or "
                    + "delete the index [test] in order to retry to restore the snapshot again or use the reroute API "
                    + "to force the allocation of an empty primary shard. Details: [restore_source[_repository/_missing]]"
            )
        );
    }

    public void testCanAllocatePrimaryExistingInRestoreInProgress() {
        RecoverySource.SnapshotRecoverySource recoverySource = createSnapshotRecoverySource("_existing");

        ClusterState clusterState = createInitialClusterState();
        RoutingTable routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY, clusterState.getRoutingTable())
            .addAsRestore(clusterState.getMetadata().index("test"), recoverySource)
            .build();

        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        ShardRouting primary = clusterState.getRoutingTable().shardRoutingTable("test", 0).primaryShard();
        assertEquals(ShardRoutingState.UNASSIGNED, primary.state());
        assertEquals(RecoverySource.Type.SNAPSHOT, primary.recoverySource().getType());

        routingTable = clusterState.routingTable();

        final RestoreInProgress.State shardState;
        if (randomBoolean()) {
            shardState = randomFrom(RestoreInProgress.State.STARTED, RestoreInProgress.State.INIT);
        } else {
            shardState = RestoreInProgress.State.FAILURE;

            UnassignedInfo currentInfo = primary.unassignedInfo();
            UnassignedInfo newInfo = new UnassignedInfo(
                currentInfo.getReason(),
                currentInfo.getMessage(),
                new IOException("i/o failure"),
                currentInfo.getNumFailedAllocations(),
                currentInfo.getUnassignedTimeInNanos(),
                currentInfo.getUnassignedTimeInMillis(),
                currentInfo.isDelayed(),
                currentInfo.getLastAllocationStatus(),
                currentInfo.getFailedNodeIds(),
                currentInfo.getLastAllocatedNodeId()
            );
            primary = primary.updateUnassigned(newInfo, primary.recoverySource());

            IndexRoutingTable indexRoutingTable = routingTable.index("test");
            IndexRoutingTable.Builder newIndexRoutingTable = IndexRoutingTable.builder(indexRoutingTable.getIndex());
            for (int shardId = 0; shardId < indexRoutingTable.size(); shardId++) {
                IndexShardRoutingTable shardRoutingTable = indexRoutingTable.shard(shardId);
                for (int copy = 0; copy < shardRoutingTable.size(); copy++) {
                    ShardRouting shardRouting = shardRoutingTable.shard(copy);
                    if (shardRouting.primary()) {
                        newIndexRoutingTable.addShard(primary);
                    } else {
                        newIndexRoutingTable.addShard(shardRouting);
                    }
                }
            }
            routingTable = RoutingTable.builder(routingTable).add(newIndexRoutingTable).build();
        }

        Map<ShardId, RestoreInProgress.ShardRestoreStatus> shards = Map.of(
            primary.shardId(),
            new RestoreInProgress.ShardRestoreStatus(clusterState.getNodes().getLocalNodeId(), shardState)
        );

        Snapshot snapshot = recoverySource.snapshot();
        RestoreInProgress.State restoreState = RestoreInProgress.State.STARTED;
        RestoreInProgress.Entry restore = new RestoreInProgress.Entry(
            recoverySource.restoreUUID(),
            snapshot,
            restoreState,
            false,
            singletonList("test"),
            shards
        );

        clusterState = ClusterState.builder(clusterState)
            .putCustom(RestoreInProgress.TYPE, new RestoreInProgress.Builder().add(restore).build())
            .routingTable(routingTable)
            .build();

        Decision decision = executeAllocation(clusterState, primary);
        if (shardState == RestoreInProgress.State.FAILURE) {
            assertEquals(Decision.Type.NO, decision.type());
            assertThat(
                decision.getExplanation(),
                startsWith(
                    "shard has failed to be restored from the snapshot [_repository:_existing/_uuid] - manually close or delete the index "
                        + "[test] in order to retry to restore the snapshot again or use the reroute API to force the allocation of "
                        + "an empty primary shard. Details: [restore_source[_repository/_existing], failure "
                        + "java.io.IOException: i/o failure"
                )
            );
        } else {
            assertEquals(Decision.Type.YES, decision.type());
            assertEquals("shard is currently being restored", decision.getExplanation());
        }
    }

    private ClusterState createInitialClusterState() {
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
            .build();

        RoutingTable routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.index("test"))
            .build();

        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder()
            .add(newNode("master", Collections.singleton(DiscoveryNodeRole.MASTER_ROLE)))
            .localNodeId("master")
            .masterNodeId("master")
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(discoveryNodes)
            .build();

        assertEquals(2, shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.UNASSIGNED).size());
        return clusterState;
    }

    private Decision executeAllocation(final ClusterState clusterState, final ShardRouting shardRouting) {
        final AllocationDecider decider = new RestoreInProgressAllocationDecider();
        final RoutingAllocation allocation = new RoutingAllocation(
            new AllocationDeciders(Collections.singleton(decider)),
            clusterState,
            null,
            null,
            0L
        );
        allocation.debugDecision(true);

        final Decision decision;
        if (randomBoolean()) {
            decision = decider.canAllocate(shardRouting, allocation);
        } else {
            DiscoveryNode node = clusterState.getNodes().getMasterNode();
            decision = decider.canAllocate(shardRouting, RoutingNodesHelper.routingNode(node.getId(), node), allocation);
        }
        return decision;
    }

    private RecoverySource.SnapshotRecoverySource createSnapshotRecoverySource(final String snapshotName) {
        Snapshot snapshot = new Snapshot("_repository", new SnapshotId(snapshotName, "_uuid"));
        return new RecoverySource.SnapshotRecoverySource(
            UUIDs.randomBase64UUID(),
            snapshot,
            Version.CURRENT,
            new IndexId("test", UUIDs.randomBase64UUID(random()))
        );
    }
}
