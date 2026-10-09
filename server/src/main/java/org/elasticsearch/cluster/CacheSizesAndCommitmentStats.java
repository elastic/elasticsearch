/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.index.shard.ShardId;

import java.util.Map;

/**
 * Node cache size and commitment stats, and individual shard cache requirements.
 */
public record CacheSizesAndCommitmentStats(
    Map<ShardId, BoostedAndUnboostedCacheRequirements> shardCacheRequirements,
    Map<String, NodeCacheSizeAndCommitments> nodeCacheSizeAndCommitments
) {

    public static final CacheSizesAndCommitmentStats EMPTY = new CacheSizesAndCommitmentStats(Map.of(), Map.of());

    public CacheSizesAndCommitmentStats {
        shardCacheRequirements = Map.copyOf(shardCacheRequirements);
        nodeCacheSizeAndCommitments = Map.copyOf(nodeCacheSizeAndCommitments);
    }
}
