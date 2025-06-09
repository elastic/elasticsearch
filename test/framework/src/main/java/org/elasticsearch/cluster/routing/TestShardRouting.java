/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;

import java.util.Set;

import static org.elasticsearch.cluster.routing.AllocationId.newInitializing;
import static org.elasticsearch.cluster.routing.AllocationId.newRelocation;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomIdentifier;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.test.ESTestCase.randomLongBetween;
import static org.elasticsearch.test.ESTestCase.randomNonNegativeLong;
import static org.elasticsearch.test.ESTestCase.randomUUID;
import static org.elasticsearch.test.ESTestCase.safeSleep;
import static org.junit.Assert.assertNotEquals;

/**
 * A helper that allows to create shard routing instances within tests, while not requiring to expose
 * different simplified constructors on the ShardRouting itself.
 *
 * Please do not add more `newShardRouting`, consider using a aSharRouting builder instead
 */
public class TestShardRouting {

    private static final Logger logger = LogManager.getLogger(TestShardRouting.class);

    public static Builder shardRoutingBuilder(String index, int shardId, String currentNodeId, boolean primary, ShardRoutingState state) {
        return shardRoutingBuilder(new ShardId(index, IndexMetadata.INDEX_UUID_NA_VALUE, shardId), currentNodeId, primary, state);
    }

    public static Builder shardRoutingBuilder(ShardId shardId, String currentNodeId, boolean primary, ShardRoutingState state) {
        return new Builder(shardId, currentNodeId, primary, state);
    }

    public static class Builder {

        private final ShardId shardId;
        private String currentNodeId;
        private String relocatingNodeId;
        private boolean primary;
        private ShardRoutingState state;
        private RecoverySource recoverySource;
        private UnassignedInfo unassignedInfo;
        private RelocationFailureInfo relocationFailureInfo;
        private AllocationId allocationId;
        private Long expectedShardSize;
        private ShardRouting.Role role;

        public Builder(ShardId shardId, String currentNodeId, boolean primary, ShardRoutingState state) {
            this.shardId = shardId;
            this.currentNodeId = currentNodeId;
            this.primary = primary;
            this.state = state;
        }

        public Builder withRelocatingNodeId(String relocatingNodeId) {
            this.relocatingNodeId = relocatingNodeId;
            return this;
        }

        public Builder withRecoverySource(RecoverySource recoverySource) {
            this.recoverySource = recoverySource;
            return this;
        }

        public Builder withUnassignedInfo(UnassignedInfo unassignedInfo) {
            this.unassignedInfo = unassignedInfo;
            return this;
        }

        public Builder withRelocationFailureInfo(RelocationFailureInfo relocationFailureInfo) {
            this.relocationFailureInfo = relocationFailureInfo;
            return this;
        }

        public Builder withAllocationId(AllocationId allocationId) {
            this.allocationId = allocationId;
            return this;
        }

        public Builder withExpectedShardSize(Long expectedShardSize) {
            this.expectedShardSize = expectedShardSize;
            return this;
        }

        public Builder withRole(ShardRouting.Role role) {
            this.role = role;
            return this;
        }

        public ShardRouting build() {
            return new ShardRouting(
                shardId,
                currentNodeId,
                relocatingNodeId,
                primary,
                state,
                recoverySource != null ? recoverySource : buildRecoverySource(primary, state),
                unassignedInfo != null ? unassignedInfo : buildUnassignedInfo(state),
                relocationFailureInfo != null ? relocationFailureInfo : buildRelocationFailureInfo(state),
                allocationId != null ? allocationId : buildAllocationId(state),
                expectedShardSize != null ? expectedShardSize : ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE,
                role != null ? role : ShardRouting.Role.DEFAULT
            );
        }
    }

    public static ShardRouting newShardRouting(String index, int shardId, String currentNodeId, boolean primary, ShardRoutingState state) {
        return newShardRouting(new ShardId(index, IndexMetadata.INDEX_UUID_NA_VALUE, shardId), currentNodeId, primary, state);
    }

    public static ShardRouting newShardRouting(ShardId shardId, String currentNodeId, boolean primary, ShardRoutingState state) {
        return newShardRouting(shardId, currentNodeId, primary, state, ShardRouting.Role.DEFAULT);
    }

    public static ShardRouting newShardRouting(
        ShardId shardId,
        String currentNodeId,
        boolean primary,
        ShardRoutingState state,
        ShardRouting.Role role
    ) {
        assertNotEquals(ShardRoutingState.RELOCATING, state);
        return new ShardRouting(
            shardId,
            currentNodeId,
            null,
            primary,
            state,
            buildRecoverySource(primary, state),
            buildUnassignedInfo(state),
            buildRelocationFailureInfo(state),
            buildAllocationId(state),
            ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE,
            role
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
            state
        );
    }

    public static ShardRouting newShardRouting(
        ShardId shardId,
        String currentNodeId,
        String relocatingNodeId,
        boolean primary,
        ShardRoutingState state
    ) {
        return new ShardRouting(
            shardId,
            currentNodeId,
            relocatingNodeId,
            primary,
            state,
            buildRecoverySource(primary, state),
            buildUnassignedInfo(state),
            buildRelocationFailureInfo(state),
            buildAllocationId(state),
            ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE,
            ShardRouting.Role.DEFAULT
        );
    }

    public static RecoverySource buildRecoverySource(boolean primary, ShardRoutingState state) {
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
            case INITIALIZING, STARTED -> newInitializing(randomUUID());
            case RELOCATING -> newRelocation(newInitializing(randomUUID()));
        };
    }

    public static UnassignedInfo buildUnassignedInfo(ShardRoutingState state) {
        return switch (state) {
            case UNASSIGNED, INITIALIZING -> buildUnassignedInfo("auto generated for test");
            case STARTED, RELOCATING -> null;
        };
    }

    public static RelocationFailureInfo buildRelocationFailureInfo(ShardRoutingState state) {
        return switch (state) {
            case UNASSIGNED, INITIALIZING, STARTED -> RelocationFailureInfo.NO_FAILURES;
            case RELOCATING -> randomBoolean() ? RelocationFailureInfo.NO_FAILURES : new RelocationFailureInfo(randomIntBetween(1, 10));
        };
    }

    public static UnassignedInfo buildUnassignedInfo(String message) {
        UnassignedInfo.Reason reason = randomFrom(UnassignedInfo.Reason.values());
        String lastAllocatedNodeId = null;
        boolean delayed = false;
        if (reason == UnassignedInfo.Reason.NODE_LEFT || reason == UnassignedInfo.Reason.NODE_RESTARTING) {
            if (randomBoolean()) {
                delayed = true;
            }
            lastAllocatedNodeId = randomIdentifier();
        }
        int failedAllocations = reason == UnassignedInfo.Reason.ALLOCATION_FAILED ? 1 : 0;

        long unassignedTimeMillis = randomNonNegativeLong();
        long unassignedTimeNanos = randomLongBetween(0L, 1_000_000_000);
        ensureInPast(unassignedTimeNanos);

        return new UnassignedInfo(
            reason,
            message,
            null,
            failedAllocations,
            unassignedTimeNanos,
            unassignedTimeMillis,
            delayed,
            UnassignedInfo.AllocationStatus.NO_ATTEMPT,
            Set.of(),
            lastAllocatedNodeId
        );
    }

    /**
     * This ensures that deterministically selected nano time is actually in past to avoid unassigned info code constraints
     */
    private static void ensureInPast(long nanoTime) {
        while (System.nanoTime() < nanoTime) {
            logger.info("Waiting to ensure selected nano-time [{}] is in past", nanoTime);
            safeSleep(1000);
        }
    }

    public static RecoverySource buildRecoverySource() {
        return randomFrom(
            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            RecoverySource.ExistingStoreRecoverySource.INSTANCE,
            RecoverySource.PeerRecoverySource.INSTANCE,
            RecoverySource.LocalShardsRecoverySource.INSTANCE,
            new RecoverySource.SnapshotRecoverySource(
                randomUUID(),
                new Snapshot("repo", new SnapshotId(randomIdentifier(), randomUUID())),
                IndexVersion.current(),
                new IndexId("some_index", randomUUID())
            ),
            new RecoverySource.ReshardSplitTargetRecoverySource(new ShardId("some_index", randomUUID(), randomIntBetween(0, 1000)))
        );
    }
}
