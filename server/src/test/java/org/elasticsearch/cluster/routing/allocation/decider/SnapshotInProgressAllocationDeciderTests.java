/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SnapshotsInProgress;
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
import org.elasticsearch.test.ESTestCase;

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
            "waiting for snapshotting of shard [" + shardId + "] to complete on this node [" + nodeId + "]",
            decision.getExplanation()
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
                randomAlphaOfLength(10),
                ShardGeneration.newGeneration(random())
            );
        } else {
            shardSnapshotStatus = new SnapshotsInProgress.ShardSnapshotStatus(nodeId, shardState, ShardGeneration.newGeneration(random()));
        }
        return SnapshotsInProgress.EMPTY.withUpdatedEntriesForRepo(
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
