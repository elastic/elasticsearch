/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

/**
 * Summarizes the impact to the cluster as a result of a rebalancing round.
 *
 * @param numberOfShardsToMove The number of shard moves required to move from the previous desired balance to the new one.
 */
public record BalancingRoundSummary(long numberOfShardsToMove) {

    @Override
    public String toString() {
        return "BalancingRoundSummary{" + "numberOfShardsToMove=" + numberOfShardsToMove + '}';
    }

}
