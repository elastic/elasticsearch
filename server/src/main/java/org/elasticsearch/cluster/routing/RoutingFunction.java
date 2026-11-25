/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

/// Calculates shard that a document identified by the provided hash should be routed to.
public sealed interface RoutingFunction permits RoutingFunction.ModuloRoutingFunction, RoutingFunction.LegacyRoutingFunction {
    int shardNum(int hash);

    /// Converts a hash into a shard number using modulo operation.
    static RoutingFunction moduloNumberOfShards(int numberOfShards) {
        return new ModuloRoutingFunction(numberOfShards);
    }

    /// Converts a hash into a shard number using the old routingNumShards mechanism.
    static RoutingFunction legacyRoutingNumberOfShards(int routingNumShards, int routingFactor) {
        return new LegacyRoutingFunction(routingNumShards, routingFactor);
    }

    record ModuloRoutingFunction(int numberOfShards) implements RoutingFunction {
        @Override
        public int shardNum(int hash) {
            return Math.floorMod(hash, numberOfShards);
        }
    }

    record LegacyRoutingFunction(int routingNumShards, int routingFactor) implements RoutingFunction {
        @Override
        public int shardNum(int hash) {
            return Math.floorMod(hash, routingNumShards) / routingFactor;
        }
    }
}
