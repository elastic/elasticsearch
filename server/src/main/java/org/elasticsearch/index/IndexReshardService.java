/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexReshardingMetadata;
import org.elasticsearch.cluster.metadata.IndexReshardingState;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.index.shard.IndexShard;

import java.util.HashSet;
import java.util.Set;

/**
 * Resharding is currently only implemented in Serverless. This service only exposes minimal metadata about resharding
 * needed by other services.
 */
public class IndexReshardService {
    public static TransportVersion RESHARDING_SHARD_SUMMARY_IN_ESQL = TransportVersion.fromName("resharding_shard_summary_in_esql");

    /**
     * Returns the indices from the provided set that are currently being resharded.
     */
    public static Set<Index> reshardingIndices(final ProjectState projectState, final Set<Index> indicesToCheck) {
        final Set<Index> indices = new HashSet<>();
        for (Index index : indicesToCheck) {
            IndexMetadata indexMetadata = projectState.metadata().index(index);

            if (indexMetadata != null && indexMetadata.getReshardingMetadata() != null) {
                indices.add(index);
            }
        }
        return indices;
    }

    /// Determines if a shard snapshot is impacted by an ongoing resharding operation.
    /// Such shard snapshot may contain data inconsistent with other shards due to metadata changes or data movement
    /// performed in scope of resharding.
    /// @param maximumShardIdForIndexInTheSnapshot maximum [ShardId] by [ShardId#id()] of the same index as the provided `indexShard`
    /// that is present in the snapshot metadata. This value is used to detect previously completed resharding operations.
    public static boolean isShardSnapshotImpactedByResharding(IndexMetadata indexMetadata, int maximumShardIdForIndexInTheSnapshot) {
        // Presence of resharding metadata obviously means that the snapshot is impacted.
        if (indexMetadata.getReshardingMetadata() != null) {
            return true;
        } else if (maximumShardIdForIndexInTheSnapshot < indexMetadata.getNumberOfShards() - 1) {
            // However resharding metadata may not be present because resharding has completed already
            // but still could have impacted this snapshot.
            // If a snapshot doesn't contain the shard with maximum id
            // given the number of shards in the index metadata it means that there was a resharding
            // that increased the number of shards since the snapshot was created.
            // E.g. with resharding split 4 -> 8 an up-to-date snapshot would contain shard id 7.
            // But a snapshot based on a pre-resharding index metadata would only contain shard id 3.
            // Without resharding every snapshot would always contain the maximum shard id.
            return true;
        } else {
            return false;
        }
    }

    /// Determines if an operation with the provided summary is impacted by an ongoing resharding split.
    /// This is currently specifically designed for realtime read operations like term vectors API.
    /// As such it is intended to be called in scope of the realtime read operation on the _index_ shard.
    public static boolean isRealtimeReadPossiblyStale(IndexShard indexShard, SplitShardCountSummary splitShardCountSummary) {
        if (splitShardCountSummary.isUnset()) {
            // If the coordinator didn't provide the summary it won't know how to perform retries either.
            // So we don't retry.
            return false;
        }

        IndexMetadata indexMetadata = indexShard.indexSettings().getIndexMetadata();

        return switch (splitShardCountSummary.check(indexMetadata)) {
            case OLDER -> {
                IndexReshardingMetadata reshardingMetadata = indexMetadata.getReshardingMetadata();
                // Otherwise it would be INVALID.
                assert reshardingMetadata != null;

                // If we see an older summary it means that this request was routed to the source shard, possibly
                // due to the target shard being not ready as seen by the coordinator.
                // However, it is possible that this document has moved to the target shard since (and maybe was updated).
                // But that is only a concern if we are post handoff.
                // If handoff didn't start yet, we can trust the data that the source shard has since all writes are still performed
                // by the source.
                // If handoff is in progress, writes are queued and we again can trust the data that the source shard has so far.
                // We will only unblock writes once we observe HANDOFF state of the target shard locally on the source index shard
                // (which is where we assume we are) and therefore we can trust what we read from index metadata.
                assert reshardingMetadata.isSplit();
                IndexReshardingState.Split split = reshardingMetadata.getSplit();
                assert split.isSourceShard(indexShard.shardId().id());

                int targetShard = split.targetShard(indexShard.shardId().id());
                yield reshardingMetadata.getSplit().targetStateAtLeast(targetShard, IndexReshardingState.Split.TargetShardState.HANDOFF);
            }
            case CURRENT -> false;
            case INVALID -> true;
        };
    }
}
