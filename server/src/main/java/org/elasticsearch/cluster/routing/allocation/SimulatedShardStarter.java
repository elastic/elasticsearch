/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.routing.ShardRouting;

/**
 * Lets a simulation start each shard as soon as the allocator initializes it (a new copy or a relocation target), instead of leaving it
 * in flight until the allocator returns. The allocator can then go on to decide further changes against the simulated result of the
 * earlier ones, so that a single {@code allocate()} makes many changes, and nothing is ever in flight to be throttled.
 */
public interface SimulatedShardStarter {

    /**
     * Whether the allocator should carry on making changes within this call. It is asked after each relocation, and once it has said
     * {@code false} the allocator finishes the pass it is in without making any further relocations and returns to the simulation. This
     * bounds the time between the simulation's own checks, for example that its result is still wanted.
     */
    boolean shouldContinue();

    /**
     * Starts a shard that the allocator has just initialized, and updates the simulated {@link org.elasticsearch.cluster.ClusterInfo} on
     * the {@link RoutingAllocation} to reflect it.
     *
     * @param initializingShard the new copy or relocation target, in state INITIALIZING
     * @return the started shard routing
     */
    ShardRouting startShard(ShardRouting initializingShard);
}
