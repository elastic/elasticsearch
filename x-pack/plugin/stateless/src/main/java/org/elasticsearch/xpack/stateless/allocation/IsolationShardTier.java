/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.allocation;

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.core.Nullable;

import static org.elasticsearch.cluster.routing.ShardRouting.Role.INDEX_ONLY;
import static org.elasticsearch.cluster.routing.ShardRouting.Role.SEARCH_ONLY;

/**
 * Stateless shard tiers that participate in project isolation
 */
public enum IsolationShardTier {
    INDEX,
    SEARCH;

    /**
     * Maps a shard routing role to an isolation tier, or {@code null} if this decider should not apply.
     */
    public static @Nullable IsolationShardTier fromShardRole(ShardRouting.Role shardRoutingRole) {
        if (shardRoutingRole == INDEX_ONLY) {
            return INDEX;
        }
        if (shardRoutingRole == SEARCH_ONLY) {
            return SEARCH;
        }
        return null;
    }

    /** Key used under the cluster setting {@code tiers} object ({@code index} / {@code search}). */
    public String isolationTierName() {
        return this == INDEX ? "index" : "search";
    }
}
