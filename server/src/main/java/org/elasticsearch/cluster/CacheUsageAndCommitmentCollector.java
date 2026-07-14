/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.index.shard.ShardId;

import java.util.Map;

/**
 * Collects shared cache sizing and usage information for {@link ClusterInfo}.
 */
public interface CacheUsageAndCommitmentCollector {

    /**
     * Used when no cache usage and commitment collector is available.
     */
    CacheUsageAndCommitmentCollector EMPTY = new CacheUsageAndCommitmentCollector() {
        @Override
        public void collectShardCacheSizes(
            ClusterState clusterState,
            ActionListener<Map<ShardId, BoostedAndUnboostedCacheSizes>> listener
        ) {
            listener.onResponse(Map.of());
        }

        @Override
        public void collectNodeCacheStats(ClusterState clusterState, ActionListener<Map<String, NodeCacheStats>> listener) {
            listener.onResponse(Map.of());
        }
    };

    /**
     * Collects the boosted and unboosted cache size commitment for every shard with cache sizing information.
     *
     * @param clusterState The cluster state snapshot for this collection.
     * @param listener The listener which will receive the results.
     */
    void collectShardCacheSizes(ClusterState clusterState, ActionListener<Map<ShardId, BoostedAndUnboostedCacheSizes>> listener);

    /**
     * Collects the current cache stats for every node with cache sizing information.
     *
     * @param clusterState The cluster state snapshot for this collection.
     * @param listener The listener which will receive the results.
     */
    void collectNodeCacheStats(ClusterState clusterState, ActionListener<Map<String, NodeCacheStats>> listener);
}
