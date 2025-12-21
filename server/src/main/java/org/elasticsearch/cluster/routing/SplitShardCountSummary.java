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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

/**
 * The SplitShardCountSummary has been added to accommodate in-place index resharding.
 * <p>
 * Documents are routed to shards by hashing some routing field of the document
 * (typically the document ID) and partitioning the hash space among the shards.
 * When the number of shards changes, that changes the way the hash space is partitioned,
 * causing some documents to route to different shards. For documents that have already been indexed,
 * the shards coordinate between themselves to move documents from their old shard to their new home
 * as necessary. The original shard hands off the share of its hash space that will route to the new
 * shard atomically, blocking requests while the handoff is performed and rerouting any queued requests
 * that should now route to the new shard after handoff has completed.
 * <p>
 * But how does the original shard know if a queued request should be rerouted? And for that matter, how
 * does it know whether requests that arrive after handoff has completed have been routed correctly?
 * It is the coordinating node's job to convert index-level requests into the appropriate set of shard-level requests.
 * That's where SplitShardCountSummary comes in. The coordinator computes this field for each shard-level request
 * it creates, based on its view of the number of routable shards in the index at the time. When the receiving shard
 * processes the request, it can compare the summary provided by the coordinator against its own view. If a handoff has
 * occurred between the time the coordinator created the shard request and the time the shard processes it, then the
 * provided summary won't match the summary calculated by the receiving shard.
 * <p>
 * The SplitShardCountSummary is calculated by the coordinating node independently for each shard-level request, but its value
 * is either the old number of shards in the *index* or the new number of shards. This compact encoding still makes it possible to handle
 * requests that are stale enough that more than one split has taken place between the time the request was created and
 * processed (although this is expected to be rare, long-running requests like PITs mean that it should be handled gracefully).
 * Some indexing examples follow.
 * <p>
 * Example 1:
 * Suppose we are resharding an index from 2 -> 4 shards. While splitting a bulk request, the coordinator observes
 * that target shards are not ready for indexing. So requests that are meant for shard 0 and 2 are bundled together,
 * sent to shard 0 with “reshardSplitShardCountSummary” 2 in the request.
 * Requests that are meant for shard 1 and 3 are bundled together,
 * sent to shard 1 with “reshardSplitShardCountSummary” 2 in the request.
 * <p>
 * Example 2:
 * Suppose we are resharding an index from 4 -> 8 shards. While splitting a bulk request, the coordinator observes
 * that source shard 0 has completed handoff but source shards 1, 2, 3 have not completed handoff.
 * So, the shard-bulk-request it sends to shard 0 and 4 has the "reshardSplitShardCountSummary" 8,
 * while the shard-bulk-request it sends to shard 1,2,3 has the "reshardSplitShardCountSummary" 4.
 * Note that in this case no shard-bulk-request is sent to shards 5, 6, 7 and the requests that were meant for these target shards
 * are bundled together with and sent to their source shards.
 * <p>
 * Search during resharding has the same basic problem but is handled a little differently. Indexing shards handle stale requests by
 * re-splitting bulk requests into sub requests containing documents appropriate for each shard, then aggregating the results of those
 * requests and completing the original shard level request. But when a shard is split, it starts out with both the old and new shards
 * containing all the documents that were on the old shard, until each shard deletes the documents that route to the other at the
 * end of the split process. To handle search requests that arrive after the split but before the cleanup, search shards filter out
 * documents that belong to the other shard, but only if they know that the coordinator is including the other shard in its query.
 * If the coordinator's shard count summary differs from the shard's calculation, that means that the coordinator did not include
 * shards in the query that have been split since the query was created, and the search shards should not filter. If the count matches,
 * then the coordinator is including both the old and new shards in its query, and the shards themselves should filter.
 * <p>
 * Search example:
 * Suppose we are resharding an index from 4 -> 8 shards. While handling a search request, the coordinator observes
 * that target shard 5 is in SPLIT state but target shards 4, 6, 7 are in CLONE/HANDOFF state.
 * The coordinator will send shard search requests to all source shards (0, 1, 2, 3) and to all target shards
 * that are at least in SPLIT state (5).
 * Shard search request sent to source shards 0, 2, 3 has the "reshardSplitShardCountSummary" of 4
 * since corresponding target shards (4, 6, 7) have not advanced to SPLIT state.
 * Shard search request sent to source shard 1 has the "reshardSplitShardCountSummary" of 8
 * since the corresponding target shard 5 is in SPLIT state.
 * When a shard search request is executed on the source shard 1, "reshardSplitShardCountSummary" value
 * is checked and documents that will be returned by target shard 5 are excluded
 * (they are still present in the source shard because the resharding process is not complete).
 * All other source shard search requests (0, 2, 3) return all available documents since corresponding target shards
 * are not yet available to do that.
 * <p>
 * A value of 0 indicates an INVALID reshardSplitShardCountSummary. Hence, a request with INVALID reshardSplitShardCountSummary
 * will be treated as a Summary mismatch on the source shard node.
 */

public class SplitShardCountSummary implements Writeable {
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
     * Used for deserialization in versions that use int instead of vInt for serialization.
     */
    public static SplitShardCountSummary fromInt(int payload) {
        return new SplitShardCountSummary(payload);
    }

    private final int shardCountSummary;

    /**
     * Deserialize a SplitShardCountSummary using a canonical vInt-based serialization protocol.
     */
    public SplitShardCountSummary(StreamInput in) throws IOException {
        this.shardCountSummary = in.readVInt();
    }

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

    /**
     * Serializes a SplitShardCountSummary using a canonical vInt-based serialization protocol.
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(shardCountSummary);
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

    @Override
    public String toString() {
        return "SplitShardCountSummary [shardCountSummary=" + shardCountSummary + "]";
    }
}
