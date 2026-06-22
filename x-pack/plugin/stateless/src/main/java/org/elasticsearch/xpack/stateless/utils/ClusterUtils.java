/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.utils;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.shard.ShardId;

/**
 * Cluster utilities for stateless nodes.
 */
public final class ClusterUtils {

    private ClusterUtils() {}

    /**
     * Returns {@code true} if the shard is assigned to the local node, including as a relocation target.
     */
    public static boolean isShardLocallyAllocated(ClusterService clusterService, ShardId shardId) {
        final var state = clusterService.state();
        final String localNodeId = state.nodes().getLocalNodeId();
        if (localNodeId == null) {
            return false;
        }
        final var routingNode = state.getRoutingNodes().node(localNodeId);
        return routingNode != null && routingNode.getByShardId(shardId) != null;
    }
}
