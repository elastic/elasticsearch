/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;

import java.util.Collections;

/**
 * A factory that produces {@link Iterable}s of {@link ShardRouting}s used to look for non-preferred allocation and
 * try to relocate them. The first shard encountered that the allocation deciders indicate is in a <code>NOT_PREFERRED</code>
 * allocation, and can be moved to a preferred allocation, will be moved and the iteration will stop.
 */
public interface NonPreferredShardIteratorFactory {

    /**
     * Doesn't iterate over the shards at all, can be used to disable movement of <code>NON_PREFERRED</code> shards.
     */
    NonPreferredShardIteratorFactory NOOP = ignored -> Collections.emptyList();

    /**
     * Just iterates over all shards using {@link org.elasticsearch.cluster.routing.RoutingNodes#nodeInterleavedShardIterator()}
     */
    NonPreferredShardIteratorFactory NODE_INTERLEAVED = allocation -> () -> allocation.routingNodes().nodeInterleavedShardIterator();

    /**
     * Create an iterator returning all shards to be checked for non-preferred allocation, ordered in
     * descending desirability-to-move order
     *
     * @param allocation the current routing allocation
     * @return An iterator containing shards we'd like to move to a preferred allocation
     */
    Iterable<ShardRouting> createNonPreferredShardIterator(RoutingAllocation allocation);
}
