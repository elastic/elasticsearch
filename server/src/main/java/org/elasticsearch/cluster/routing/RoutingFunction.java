/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

/// Calculates shard id for a document identified by the provided hash.
@FunctionalInterface
public interface RoutingFunction {
    int shardId(int hash);

    /// Converts a hash into a shard id using modulo operation.
    static RoutingFunction moduloNumberOfShards(int numberOfShards) {
        return hash -> Math.floorMod(hash, numberOfShards);
    }

    /// Converts a hash into a shard id using the old routingNumShards mechanism.
    static RoutingFunction legacyRoutingNumberOfShards(int routingNumShards, int routingFactor) {
        return hash -> Math.floorMod(hash, routingNumShards) / routingFactor;
    }
}
