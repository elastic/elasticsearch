/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.seqno;

import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.SafeCommitInfo;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;

import java.util.Set;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

public abstract class ReplicationTrackerTestCase extends ESTestCase {

    ReplicationTracker newTracker(
        final AllocationId allocationId,
        final LongConsumer updatedGlobalCheckpoint,
        final LongSupplier currentTimeMillisSupplier
    ) {
        return new ReplicationTracker(
            new ShardId("test", "_na_", 0),
            allocationId.getId(),
            IndexSettingsModule.newIndexSettings("test", Settings.EMPTY),
            randomNonNegativeLong(),
            UNASSIGNED_SEQ_NO,
            updatedGlobalCheckpoint,
            currentTimeMillisSupplier,
            (leases, listener) -> {},
            OPS_BASED_RECOVERY_ALWAYS_REASONABLE
        );
    }

    static final Supplier<SafeCommitInfo> OPS_BASED_RECOVERY_ALWAYS_REASONABLE = () -> SafeCommitInfo.EMPTY;

    static String nodeIdFromAllocationId(final AllocationId allocationId) {
        return "n-" + allocationId.getId().substring(0, 8);
    }

    static IndexShardRoutingTable routingTable(final Set<AllocationId> initializingIds, final AllocationId primaryId) {
        final ShardId shardId = new ShardId("test", "_na_", 0);
        final ShardRouting primaryShard = TestShardRouting.newShardRouting(
            shardId,
            nodeIdFromAllocationId(primaryId),
            null,
            true,
            ShardRoutingState.STARTED,
            primaryId
        );
        return routingTable(initializingIds, primaryShard);
    }

    static IndexShardRoutingTable routingTable(final Set<AllocationId> initializingIds, final ShardRouting primaryShard) {
        assert initializingIds.contains(primaryShard.allocationId()) == false;
        final ShardId shardId = new ShardId("test", "_na_", 0);
        final IndexShardRoutingTable.Builder builder = new IndexShardRoutingTable.Builder(shardId);
        for (final AllocationId initializingId : initializingIds) {
            builder.addShard(
                TestShardRouting.newShardRouting(
                    shardId,
                    nodeIdFromAllocationId(initializingId),
                    null,
                    false,
                    ShardRoutingState.INITIALIZING,
                    initializingId
                )
            );
        }

        builder.addShard(primaryShard);

        return builder.build();
    }

}
