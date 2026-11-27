/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.ShardId;

/**
 * One placement-dependent node aggregate of {@link ClusterInfo} (used bytes, cache commitment, write load, ...), maintained as
 * <pre>
 *   A(node) = polled(node) + sum(f(s) for s currently placed on node) - sum(f(s) for s placed on node when ClusterInfo was polled)
 * </pre>
 * where {@code f} is a placement-independent fact about the shard. A {@link RoutingDerivedPlacementModel} feeds every aspect the
 * same three kinds of event; an aspect keeps its own running per-node delta and applies it to the polled values in {@link #simulated}.
 *
 * @param <R> the simulated value, typically the same type as the corresponding {@link ClusterInfo} getter returns
 */
public interface PlacementAspect<R> {

    /** A shard copy the routing table currently places on its node, other than an initializing one (adds {@code f}). */
    void shardPlaced(ShardRouting shard);

    /**
     * A shard that {@link ClusterInfo#dataPath} says was on {@code nodeId} when the poll was taken (subtracts {@code f}).
     *
     * @param currentOnNode the copy of the shard the routing table now places on that node, or {@code null} if it has left
     */
    void shardPlacedAtPoll(String nodeId, ShardId shardId, @Nullable ShardRouting currentOnNode);

    /** The computation starts an initializing shard: a relocation target ({@code relocatingNodeId} is the source) or a new copy. */
    void shardStarted(ShardRouting initializingTarget);

    /** The polled values with the accumulated delta applied. O(nodes). */
    R simulated();
}
