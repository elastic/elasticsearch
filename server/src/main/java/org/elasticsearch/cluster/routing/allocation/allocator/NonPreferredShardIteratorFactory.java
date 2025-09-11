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

public interface NonPreferredShardIteratorFactory {

    NonPreferredShardIteratorFactory NOOP = ignored -> Collections.emptyList();

    /**
     * Create an iterator returning all shards to be checked for non-preferred allocation, ordered in
     * descending desirability-to-move order
     *
     * @param allocation the current routing allocation
     * @return An iterator containing shards we'd like to move to a preferred allocation
     */
    Iterable<ShardRouting> createNonPreferredShardIterator(RoutingAllocation allocation);
}
