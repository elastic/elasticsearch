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

/**
 * NodeSorters is just a cache of
 * {@link org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator.NodeSorter}
 * instances for each cluster partition
 * <p>
 * The returned iterator will return a node sorter for each partition in the cluster.
 */
public interface NodeSorters extends Iterable<BalancedShardsAllocator.NodeSorter> {

    /**
     * Get the {@link BalancedShardsAllocator.NodeSorter} for the specified shard
     */
    BalancedShardsAllocator.NodeSorter sorterForShard(ShardRouting shard);
}
