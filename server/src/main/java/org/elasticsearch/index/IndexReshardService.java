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
}
