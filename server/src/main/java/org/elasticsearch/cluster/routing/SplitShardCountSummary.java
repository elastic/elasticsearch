/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexReshardingMetadata;
import org.elasticsearch.cluster.metadata.IndexReshardingState;

/**
 * The SplitShardCountSummary has been added to accommodate in-place index resharding.
 * This is populated when the coordinator is deciding which shards a request applies to.
 * For example, {@link org.elasticsearch.action.bulk.BulkOperation} splits
 * an incoming bulk request into shard level {@link org.elasticsearch.action.bulk.BulkShardRequest}
 * based on its cluster state view of the number of shards that are ready for indexing.
 * The purpose of this metadata is to reconcile the cluster state visible at the coordinating
 * node with that visible at the source shard node. (w.r.t resharding).
 * When an index is being split, there is a point in time when the newly created shard (target shard)
 * takes over its portion of the document space from the original shard (source shard).
 * Although the handoff is atomic at the original (source shard) and new shards (target shard),
 * there is a window of time between the coordinating node creating a shard request and the shard receiving and processing it.
 * This field is used by the original shard (source shard) when it processes the request to detect whether
 * the coordinator's view of the new shard's state when it created the request matches the shard's current state,
 * or whether the request must be reprocessed taking into account the current shard states.
 *
 * Note that we are able to get away with a single number, instead of an array of target shard states,
 * because we only allow splits in increments of 2x.
 *
 * Example 1:
 * Suppose we are resharding an index from 2 -> 4 shards. While splitting a bulk request, the coordinator observes
 * that target shards are not ready for indexing. So requests that are meant for shard 0 and 2 are bundled together,
 * sent to shard 0 with “reshardSplitShardCountSummary” 2 in the request.
 * Requests that are meant for shard 1 and 3 are bundled together,
 * sent to shard 1 with “reshardSplitShardCountSummary” 2 in the request.
 *
 * Example 2:
 * Suppose we are resharding an index from 4 -> 8 shards. While splitting a bulk request, the coordinator observes
 * that source shard 0 has completed HANDOFF but source shards 1, 2, 3 have not completed handoff.
 * So, the shard-bulk-request it sends to shard 0 and 4 has the "reshardSplitShardCountSummary" 8,
 * while the shard-bulk-request it sends to shard 1,2,3 has the "reshardSplitShardCountSummary" 4.
 * Note that in this case no shard-bulk-request is sent to shards 5, 6, 7 and the requests that were meant for these target shards
 * are bundled together with and sent to their source shards.
 *
 * A value of 0 indicates an INVALID reshardSplitShardCountSummary. Hence, a request with INVALID reshardSplitShardCountSummary
 * will be treated as a Summary mismatch on the source shard node.
 */

public class SplitShardCountSummary {
    public static final SplitShardCountSummary UNSET = new SplitShardCountSummary(0);

    /**
     * Given {@code IndexMetadata} and a {@code shardId}, this method returns the "effective" shard count
     * as seen by this IndexMetadata, for indexing operations.
     *
     * See {@code getReshardSplitShardCountSummary} for more details.
     * @param indexMetadata IndexMetadata of the shard for which we want to calculate the effective shard count
     * @param shardId       Input shardId for which we want to calculate the effective shard count
     */
    public static SplitShardCountSummary forIndexing(IndexMetadata indexMetadata, int shardId) {
        return getReshardSplitShardCountSummary(indexMetadata, shardId, IndexReshardingState.Split.TargetShardState.HANDOFF);
    }

    /**
     * This method is used in the context of the resharding feature.
     * Given a {@code shardId}, this method returns the "effective" shard count
     * as seen by this IndexMetadata, for search operations.
     *
     * See {@code getReshardSplitShardCount} for more details.
     * @param indexMetadata IndexMetadata of the shard for which we want to calculate the effective shard count
     * @param shardId  Input shardId for which we want to calculate the effective shard count
     */
    public static SplitShardCountSummary forSearch(IndexMetadata indexMetadata, int shardId) {
        return getReshardSplitShardCountSummary(indexMetadata, shardId, IndexReshardingState.Split.TargetShardState.SPLIT);
    }

    /**
     * This method is used in the context of the resharding feature.
     * Given a {@code shardId} and {@code minShardState} i.e. the minimum target shard state required for
     * an operation to be routed to target shards,
     * this method returns the "effective" shard count as seen by this IndexMetadata.
     *
     * The reshardSplitShardCountSummary tells us whether the coordinator routed requests to the source shard or
     * to both source and target shards. Requests are routed to both source and target shards
     * once the target shards are ready for an operation.
     *
     * The coordinator routes requests to source and target shards, based on its cluster state view of the state of shards
     * undergoing a resharding operation. This method is used to populate a field in the shard level requests sent to
     * source and target shards, as a proxy for the cluster state version. The same calculation is then done at the source shard
     * to verify if the coordinator and source node's view of the resharding state have a mismatch.
     * See {@link org.elasticsearch.action.support.replication.ReplicationRequest#reshardSplitShardCountSummary}
     * for a detailed description of how this value is used.
     *
     * @param shardId  Input shardId for which we want to calculate the effective shard count
     * @param minShardState Minimum target shard state required for the target to be considered ready
     * @return Effective shard count as seen by an operation using this IndexMetadata
     */
    private static SplitShardCountSummary getReshardSplitShardCountSummary(
        IndexMetadata indexMetadata,
        int shardId,
        IndexReshardingState.Split.TargetShardState minShardState
    ) {
        int numberOfShards = indexMetadata.getNumberOfShards();
        IndexReshardingMetadata reshardingMetadata = indexMetadata.getReshardingMetadata();
        assert shardId >= 0 && shardId < numberOfShards : "shardId is out of bounds";
        int shardCount = numberOfShards;
        if (reshardingMetadata != null) {
            if (reshardingMetadata.getSplit().isTargetShard(shardId)) {
                int sourceShardId = reshardingMetadata.getSplit().sourceShard(shardId);
                // Requests cannot be routed to target shards until they are ready
                assert reshardingMetadata.getSplit().allTargetStatesAtLeast(sourceShardId, minShardState) : "unexpected target state";
                shardCount = reshardingMetadata.getSplit().shardCountAfter();
            } else if (reshardingMetadata.getSplit().isSourceShard(shardId)) {
                if (reshardingMetadata.getSplit().allTargetStatesAtLeast(shardId, minShardState)) {
                    shardCount = reshardingMetadata.getSplit().shardCountAfter();
                } else {
                    shardCount = reshardingMetadata.getSplit().shardCountBefore();
                }
            }
        }
        return new SplitShardCountSummary(shardCount);
    }

    /**
     * Construct a SplitShardCountSummary from an integer
     * Used for deserialization.
     */
    public static SplitShardCountSummary fromInt(int payload) {
        return new SplitShardCountSummary(payload);
    }

    private final int shardCountSummary;

    /**
     * Return an integer representation of this summary
     * Used for serialization.
     */
    public int asInt() {
        return shardCountSummary;
    }

    /**
     * Returns whether this shard count summary is carrying an actual value or is UNSET
     */
    public boolean isUnset() {
        return this.shardCountSummary == UNSET.shardCountSummary;
    }

    // visible for testing
    SplitShardCountSummary(int shardCountSummary) {
        this.shardCountSummary = shardCountSummary;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        SplitShardCountSummary otherSummary = (SplitShardCountSummary) other;
        return this.shardCountSummary == otherSummary.shardCountSummary;
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(shardCountSummary);
    }
}
