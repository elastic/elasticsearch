/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.ShardSnapshotResult;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.elasticsearch.snapshots.SnapshotsInProgressSerializationTests;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SnapshotInProgressAllocationDeciderTests extends ESTestCase {

    private final SnapshotInProgressAllocationDecider decider = new SnapshotInProgressAllocationDecider();
    private final Index index = new Index(randomIdentifier(), randomUUID());
    private final ShardId shardId = new ShardId(index, 0);
    private final String repositoryName = randomIdentifier();
    private final Snapshot snapshot = new Snapshot(repositoryName, new SnapshotId(randomIdentifier(), randomUUID()));
    private final String nodeId = randomIdentifier();

    public void testYesWhenSimulating() {
        final var routingAllocation = new RoutingAllocation(
            new AllocationDeciders(List.of(decider)),
            ClusterState.EMPTY_STATE,
            ClusterInfo.EMPTY,
            SnapshotShardSizeInfo.EMPTY,
            randomNonNegativeLong()
        ).mutableCloneForSimulation();
        routingAllocation.setDebugMode(RoutingAllocation.DebugMode.ON);

        final var decision = decider.canAllocate(
            TestShardRouting.newShardRouting(shardId, nodeId, true, ShardRoutingState.STARTED),
            null,
            routingAllocation
        );

        assertEquals(Decision.Type.YES, decision.type());
        assertEquals("allocation is always enabled when simulating", decision.getExplanation());
    }

    public void testYesWhenNotPrimary() {
        final var routingAllocation = new RoutingAllocation(
            new AllocationDeciders(List.of(decider)),
            ClusterState.EMPTY_STATE,
            ClusterInfo.EMPTY,
            SnapshotShardSizeInfo.EMPTY,
            randomNonNegativeLong()
        );
        routingAllocation.setDebugMode(RoutingAllocation.DebugMode.ON);

        final var decision = decider.canAllocate(
            TestShardRouting.newShardRouting(shardId, nodeId, false, ShardRoutingState.STARTED),
            null,
            routingAllocation
        );

        assertEquals(Decision.Type.YES, decision.type());
        assertEquals("the shard is not being snapshotted", decision.getExplanation());
    }

    public void testYesWhenNoSnapshots() {
        final var routingAllocation = new RoutingAllocation(
            new AllocationDeciders(List.of(decider)),
            ClusterState.EMPTY_STATE,
            ClusterInfo.EMPTY,
            SnapshotShardSizeInfo.EMPTY,
            randomNonNegativeLong()
        );
        routingAllocation.setDebugMode(RoutingAllocation.DebugMode.ON);

        final var decision = decider.canAllocate(
            TestShardRouting.newShardRouting(shardId, nodeId, true, ShardRoutingState.STARTED),
            null,
            routingAllocation
        );

        assertEquals(Decision.Type.YES, decision.type());
        assertEquals("no snapshots are currently running", decision.getExplanation());
    }

    public void testYesWhenNoShardSnapshot() {
        final var routingAllocation = new RoutingAllocation(
            new AllocationDeciders(List.of(decider)),
            // snapshot in progress but not targetting this shard
            makeClusterState(new ShardId(randomIdentifier(), randomUUID(), 0), randomFrom(SnapshotsInProgress.ShardState.values())),
            ClusterInfo.EMPTY,
            SnapshotShardSizeInfo.EMPTY,
            randomNonNegativeLong()
        );
        routingAllocation.setDebugMode(RoutingAllocation.DebugMode.ON);

        final var decision = decider.canAllocate(
            TestShardRouting.newShardRouting(shardId, nodeId, true, ShardRoutingState.STARTED),
            null,
            routingAllocation
        );

        assertEquals(Decision.Type.YES, decision.type());
        assertEquals("the shard is not being snapshotted", decision.getExplanation());
    }

    public void testYesWhenShardSnapshotComplete() {
        final var routingAllocation = new RoutingAllocation(
            new AllocationDeciders(List.of(decider)),
            // snapshot in progress but complete
            makeClusterState(
                shardId,
                randomFrom(
                    Arrays.stream(SnapshotsInProgress.ShardState.values()).filter(SnapshotsInProgress.ShardState::completed).toList()
                )
            ),
            ClusterInfo.EMPTY,
            SnapshotShardSizeInfo.EMPTY,
            randomNonNegativeLong()
        );
        routingAllocation.setDebugMode(RoutingAllocation.DebugMode.ON);

        final var decision = decider.canAllocate(
            TestShardRouting.newShardRouting(shardId, nodeId, true, ShardRoutingState.STARTED),
            null,
            routingAllocation
        );

        assertEquals(Decision.Type.YES, decision.type());
        assertEquals("the shard is not being snapshotted", decision.getExplanation());
    }

    public void testYesWhenShardSnapshotOnDifferentNode() {
        final var routingAllocation = new RoutingAllocation(
            new AllocationDeciders(List.of(decider)),
            makeClusterState(shardId, randomFrom(SnapshotsInProgress.ShardState.values())),
            ClusterInfo.EMPTY,
            SnapshotShardSizeInfo.EMPTY,
            randomNonNegativeLong()
        );
        routingAllocation.setDebugMode(RoutingAllocation.DebugMode.ON);

        final var decision = decider.canAllocate(
            // shard on a different node from the snapshot in progress one
            TestShardRouting.newShardRouting(shardId, randomIdentifier(), true, ShardRoutingState.STARTED),
            null,
            routingAllocation
        );

        assertEquals(Decision.Type.YES, decision.type());
        assertEquals("the shard is not being snapshotted", decision.getExplanation());
    }

    public void testThrottleWhenSnapshotInProgress() {
        final var routingAllocation = new RoutingAllocation(
            new AllocationDeciders(List.of(decider)),
            makeClusterState(shardId, SnapshotsInProgress.ShardState.INIT),
            ClusterInfo.EMPTY,
            SnapshotShardSizeInfo.EMPTY,
            randomNonNegativeLong()
        );
        routingAllocation.setDebugMode(RoutingAllocation.DebugMode.ON);

        final var decision = decider.canAllocate(
            TestShardRouting.newShardRouting(shardId, nodeId, true, ShardRoutingState.STARTED),
            null,
            routingAllocation
        );

        assertEquals(decision.getExplanation(), Decision.Type.THROTTLE, decision.type());
        assertEquals(
            "waiting for snapshot ["
                + SnapshotsInProgress.get(routingAllocation.getClusterState()).asStream().findFirst().orElseThrow().snapshot().toString()
                + "] of shard ["
                + shardId
                + "] to complete on node ["
                + nodeId
                + "]",
            decision.getExplanation()
        );
    }

    public void testYesWhenSnapshotInProgressButShardIsPausedDueToShutdown() {

        // need to have a shard in INIT state to avoid the fast-path
        final var otherIndex = randomIdentifier();

        final var clusterStateWithShutdownMetadata = SnapshotsInProgressSerializationTests.CLUSTER_STATE_FOR_NODE_SHUTDOWNS
            .copyAndUpdateMetadata(
                mdb -> mdb.putCustom(
                    NodesShutdownMetadata.TYPE,
                    new NodesShutdownMetadata(
                        Map.of(
                            nodeId,
                            SingleNodeShutdownMetadata.builder()
                                .setNodeId(nodeId)
                                .setNodeEphemeralId(nodeId)
                                .setType(SingleNodeShutdownMetadata.Type.REMOVE)
                                .setStartedAtMillis(randomNonNegativeLong())
                                .setReason("test")
                                .build()
                        )
                    )
                )
            );
        final var snapshotsInProgress = SnapshotsInProgress.EMPTY
            // mark nodeID as shutting down for removal
            .withUpdatedNodeIdsForRemoval(clusterStateWithShutdownMetadata)
            // create a running snapshot with shardId paused
            .createCopyWithUpdatedEntriesForRepo(
                repositoryName,
                List.of(
                    SnapshotsInProgress.Entry.snapshot(
                        snapshot,
                        randomBoolean(),
                        randomBoolean(),
                        SnapshotsInProgress.State.STARTED,
                        Map.of(
                            shardId.getIndexName(),
                            new IndexId(shardId.getIndexName(), randomUUID()),
                            otherIndex,
                            new IndexId(otherIndex, randomUUID())
                        ),
                        List.of(),
                        List.of(),
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        Map.of(
                            shardId,
                            new SnapshotsInProgress.ShardSnapshotStatus(
                                nodeId,
                                SnapshotsInProgress.ShardState.PAUSED_FOR_NODE_REMOVAL,
                                ShardGeneration.newGeneration(random())
                            ),
                            new ShardId(otherIndex, randomUUID(), 0),
                            new SnapshotsInProgress.ShardSnapshotStatus(
                                nodeId,
                                SnapshotsInProgress.ShardState.INIT,
                                ShardGeneration.newGeneration(random())
                            )
                        ),
                        null,
                        Map.of(),
                        IndexVersion.current()
                    )
                )
            );

        // if the node is marked for shutdown then the shard can move

        final var routingAllocationWithShutdownMetadata = new RoutingAllocation(
            new AllocationDeciders(List.of(decider)),
            ClusterState.builder(clusterStateWithShutdownMetadata).putCustom(SnapshotsInProgress.TYPE, snapshotsInProgress).build(),
            ClusterInfo.EMPTY,
            SnapshotShardSizeInfo.EMPTY,
            randomNonNegativeLong()
        );
        routingAllocationWithShutdownMetadata.setDebugMode(RoutingAllocation.DebugMode.ON);

        final var decisionWithShutdownMetadata = decider.canAllocate(
            TestShardRouting.newShardRouting(shardId, nodeId, true, ShardRoutingState.STARTED),
            null,
            routingAllocationWithShutdownMetadata
        );

        assertEquals(Decision.Type.YES, decisionWithShutdownMetadata.type());
        assertEquals("the shard is not being snapshotted", decisionWithShutdownMetadata.getExplanation());

        // if the node is not marked for shutdown then the shard is fixed in place

        final var routingAllocationWithoutShutdownMetadata = new RoutingAllocation(
            new AllocationDeciders(List.of(decider)),
            ClusterState.builder(ClusterName.DEFAULT).putCustom(SnapshotsInProgress.TYPE, snapshotsInProgress).build(),
            ClusterInfo.EMPTY,
            SnapshotShardSizeInfo.EMPTY,
            randomNonNegativeLong()
        );
        routingAllocationWithoutShutdownMetadata.setDebugMode(RoutingAllocation.DebugMode.ON);

        final var decisionWithoutShutdownMetadata = decider.canAllocate(
            TestShardRouting.newShardRouting(shardId, nodeId, true, ShardRoutingState.STARTED),
            null,
            routingAllocationWithoutShutdownMetadata
        );

        assertEquals(Decision.Type.THROTTLE, decisionWithoutShutdownMetadata.type());
        assertThat(
            decisionWithoutShutdownMetadata.getExplanation(),
            Matchers.matchesRegex("waiting for snapshot .* of shard .* to complete on node .*")
        );
    }

    private ClusterState makeClusterState(ShardId shardId, SnapshotsInProgress.ShardState shardState) {
        return ClusterState.builder(ClusterName.DEFAULT)
            .putCustom(SnapshotsInProgress.TYPE, makeSnapshotsInProgress(shardId, shardState))
            .build();
    }

    private SnapshotsInProgress makeSnapshotsInProgress(ShardId snapshotShardId, SnapshotsInProgress.ShardState shardState) {
        final SnapshotsInProgress.ShardSnapshotStatus shardSnapshotStatus;
        if (shardState == SnapshotsInProgress.ShardState.SUCCESS) {
            shardSnapshotStatus = SnapshotsInProgress.ShardSnapshotStatus.success(
                nodeId,
                new ShardSnapshotResult(ShardGeneration.newGeneration(random()), ByteSizeValue.ZERO, 1)
            );
        } else if (shardState == SnapshotsInProgress.ShardState.QUEUED) {
            shardSnapshotStatus = new SnapshotsInProgress.ShardSnapshotStatus(null, shardState, null);
        } else if (shardState.failed()) {
            shardSnapshotStatus = new SnapshotsInProgress.ShardSnapshotStatus(
                nodeId,
                shardState,
                ShardGeneration.newGeneration(random()),
                randomAlphaOfLength(10)
            );
        } else {
            shardSnapshotStatus = new SnapshotsInProgress.ShardSnapshotStatus(nodeId, shardState, ShardGeneration.newGeneration(random()));
        }
        return SnapshotsInProgress.EMPTY.createCopyWithUpdatedEntriesForRepo(
            repositoryName,
            List.of(
                SnapshotsInProgress.Entry.snapshot(
                    snapshot,
                    randomBoolean(),
                    randomBoolean(),
                    shardState.completed() ? SnapshotsInProgress.State.SUCCESS : SnapshotsInProgress.State.STARTED,
                    Map.of(snapshotShardId.getIndexName(), new IndexId(snapshotShardId.getIndexName(), randomUUID())),
                    List.of(),
                    List.of(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    Map.of(snapshotShardId, shardSnapshotStatus),
                    null,
                    Map.of(),
                    IndexVersion.current()
                )
            )
        );
    }
}
