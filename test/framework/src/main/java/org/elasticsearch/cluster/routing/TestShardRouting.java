/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;

import java.util.Collections;

import static org.apache.lucene.tests.util.LuceneTestCase.random;
import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;

/**
 * A helper that allows to create shard routing instances within tests, while not requiring to expose
 * different simplified constructors on the ShardRouting itself.
 */
public class TestShardRouting {

    public static ShardRouting newShardRouting(String index, int shardId, String currentNodeId, boolean primary, ShardRoutingState state) {
        return newShardRouting(new ShardId(index, IndexMetadata.INDEX_UUID_NA_VALUE, shardId), currentNodeId, primary, state);
    }

    public static ShardRouting newShardRouting(ShardId shardId, String currentNodeId, boolean primary, ShardRoutingState state) {
        return new ShardRouting(
            shardId,
            currentNodeId,
            null,
            primary,
            state,
            buildRecoveryTarget(primary, state),
            buildUnassignedInfo(state),
            buildRelocationFailureInfo(state),
            buildAllocationId(state),
            -1,
            ShardRouting.Role.DEFAULT
        );
    }

    public static ShardRouting newShardRouting(
        ShardId shardId,
        String currentNodeId,
        boolean primary,
        ShardRoutingState state,
        RecoverySource recoverySource
    ) {
        return new ShardRouting(
            shardId,
            currentNodeId,
            null,
            primary,
            state,
            recoverySource,
            buildUnassignedInfo(state),
            buildRelocationFailureInfo(state),
            buildAllocationId(state),
            -1,
            ShardRouting.Role.DEFAULT
        );
    }

    public static ShardRouting newShardRouting(
        String index,
        int shardId,
        String currentNodeId,
        String relocatingNodeId,
        boolean primary,
        ShardRoutingState state
    ) {
        return newShardRouting(
            new ShardId(index, IndexMetadata.INDEX_UUID_NA_VALUE, shardId),
            currentNodeId,
            relocatingNodeId,
            primary,
            state,
            ShardRouting.Role.DEFAULT
        );
    }

    public static ShardRouting newShardRouting(
        String index,
        int shardId,
        String currentNodeId,
        String relocatingNodeId,
        boolean primary,
        ShardRoutingState state,
        ShardRouting.Role role
    ) {
        return newShardRouting(
            new ShardId(index, IndexMetadata.INDEX_UUID_NA_VALUE, shardId),
            currentNodeId,
            relocatingNodeId,
            primary,
            state,
            role
        );
    }

    public static ShardRouting newShardRouting(
        ShardId shardId,
        String currentNodeId,
        String relocatingNodeId,
        boolean primary,
        ShardRoutingState state
    ) {
        return newShardRouting(shardId, currentNodeId, relocatingNodeId, primary, state, ShardRouting.Role.DEFAULT);
    }

    public static ShardRouting newShardRouting(
        ShardId shardId,
        String currentNodeId,
        String relocatingNodeId,
        boolean primary,
        ShardRoutingState state,
        ShardRouting.Role role
    ) {
        return new ShardRouting(
            shardId,
            currentNodeId,
            relocatingNodeId,
            primary,
            state,
            buildRecoveryTarget(primary, state),
            buildUnassignedInfo(state),
            buildRelocationFailureInfo(state),
            buildAllocationId(state),
            -1,
            role
        );
    }

    public static ShardRouting newShardRouting(
        String index,
        int shardId,
        String currentNodeId,
        String relocatingNodeId,
        boolean primary,
        ShardRoutingState state,
        AllocationId allocationId
    ) {
        return newShardRouting(
            new ShardId(index, IndexMetadata.INDEX_UUID_NA_VALUE, shardId),
            currentNodeId,
            relocatingNodeId,
            primary,
            state,
            allocationId
        );
    }

    public static ShardRouting newShardRouting(
        ShardId shardId,
        String currentNodeId,
        String relocatingNodeId,
        boolean primary,
        ShardRoutingState state,
        AllocationId allocationId
    ) {
        return new ShardRouting(
            shardId,
            currentNodeId,
            relocatingNodeId,
            primary,
            state,
            buildRecoveryTarget(primary, state),
            buildUnassignedInfo(state),
            buildRelocationFailureInfo(state),
            allocationId,
            -1,
            ShardRouting.Role.DEFAULT
        );
    }

    public static ShardRouting newShardRouting(
        String index,
        int shardId,
        String currentNodeId,
        String relocatingNodeId,
        boolean primary,
        ShardRoutingState state,
        UnassignedInfo unassignedInfo
    ) {
        return newShardRouting(index, shardId, currentNodeId, relocatingNodeId, primary, state, unassignedInfo, ShardRouting.Role.DEFAULT);
    }

    public static ShardRouting newShardRouting(
        String index,
        int shardId,
        String currentNodeId,
        String relocatingNodeId,
        boolean primary,
        ShardRoutingState state,
        UnassignedInfo unassignedInfo,
        ShardRouting.Role role
    ) {
        return newShardRouting(
            new ShardId(index, IndexMetadata.INDEX_UUID_NA_VALUE, shardId),
            currentNodeId,
            relocatingNodeId,
            primary,
            state,
            unassignedInfo,
            role
        );
    }

    public static ShardRouting newShardRouting(
        ShardId shardId,
        String currentNodeId,
        String relocatingNodeId,
        boolean primary,
        ShardRoutingState state,
        UnassignedInfo unassignedInfo
    ) {
        return newShardRouting(shardId, currentNodeId, relocatingNodeId, primary, state, unassignedInfo, ShardRouting.Role.DEFAULT);
    }

    public static ShardRouting newShardRouting(
        ShardId shardId,
        String currentNodeId,
        String relocatingNodeId,
        boolean primary,
        ShardRoutingState state,
        UnassignedInfo unassignedInfo,
        ShardRouting.Role role
    ) {
        return new ShardRouting(
            shardId,
            currentNodeId,
            relocatingNodeId,
            primary,
            state,
            buildRecoveryTarget(primary, state),
            unassignedInfo,
            buildRelocationFailureInfo(state),
            buildAllocationId(state),
            -1,
            role
        );
    }

    public static ShardRouting relocate(ShardRouting shardRouting, String relocatingNodeId, long expectedShardSize) {
        return shardRouting.relocate(relocatingNodeId, expectedShardSize);
    }

    public static RecoverySource buildRecoveryTarget(boolean primary, ShardRoutingState state) {
        return switch (state) {
            case UNASSIGNED, INITIALIZING -> primary
                ? randomFrom(RecoverySource.EmptyStoreRecoverySource.INSTANCE, RecoverySource.ExistingStoreRecoverySource.INSTANCE)
                : RecoverySource.PeerRecoverySource.INSTANCE;
            case STARTED, RELOCATING -> null;
        };
    }

    public static AllocationId buildAllocationId(ShardRoutingState state) {
        return switch (state) {
            case UNASSIGNED -> null;
            case INITIALIZING, STARTED -> AllocationId.newInitializing();
            case RELOCATING -> AllocationId.newRelocation(AllocationId.newInitializing());
        };
    }

    public static UnassignedInfo buildUnassignedInfo(ShardRoutingState state) {
        return switch (state) {
            case UNASSIGNED, INITIALIZING -> randomUnassignedInfo("auto generated for test");
            case STARTED, RELOCATING -> null;
        };
    }

    public static RelocationFailureInfo buildRelocationFailureInfo(ShardRoutingState state) {
        return switch (state) {
            case UNASSIGNED, INITIALIZING, STARTED -> RelocationFailureInfo.NO_FAILURES;
            case RELOCATING -> randomBoolean() ? RelocationFailureInfo.NO_FAILURES : new RelocationFailureInfo(randomIntBetween(1, 10));
        };
    }

    public static UnassignedInfo randomUnassignedInfo(String message) {
        UnassignedInfo.Reason reason = randomFrom(UnassignedInfo.Reason.values());
        String lastAllocatedNodeId = null;
        boolean delayed = false;
        if (reason == UnassignedInfo.Reason.NODE_LEFT || reason == UnassignedInfo.Reason.NODE_RESTARTING) {
            if (randomBoolean()) {
                delayed = true;
            }
            lastAllocatedNodeId = randomAlphaOfLength(10);
        }
        int failedAllocations = reason == UnassignedInfo.Reason.ALLOCATION_FAILED ? 1 : 0;
        return new UnassignedInfo(
            reason,
            message,
            null,
            failedAllocations,
            System.nanoTime(),
            System.currentTimeMillis(),
            delayed,
            UnassignedInfo.AllocationStatus.NO_ATTEMPT,
            Collections.emptySet(),
            lastAllocatedNodeId
        );
    }

    public static RecoverySource randomRecoverySource() {
        return randomFrom(
            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            RecoverySource.ExistingStoreRecoverySource.INSTANCE,
            RecoverySource.PeerRecoverySource.INSTANCE,
            RecoverySource.LocalShardsRecoverySource.INSTANCE,
            new RecoverySource.SnapshotRecoverySource(
                UUIDs.randomBase64UUID(),
                new Snapshot("repo", new SnapshotId(randomAlphaOfLength(8), UUIDs.randomBase64UUID())),
                IndexVersion.CURRENT,
                new IndexId("some_index", UUIDs.randomBase64UUID(random()))
            )
        );
    }
}
