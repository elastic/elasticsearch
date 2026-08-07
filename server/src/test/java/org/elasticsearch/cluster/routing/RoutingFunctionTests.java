/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.test.ESTestCase;

public class RoutingFunctionTests extends ESTestCase {
    public void testModuloSanity() {
        int shards = 1024;
        var function = RoutingFunction.moduloNumberOfShards(shards);

        int iterations = randomIntBetween(1, 10);
        int hash = 0;
        for (int i = 1; i < iterations; i++) {
            for (int shardId = 0; shardId < shards; shardId++) {
                // Since we use modulo, we get the same results in every iteration.
                assertEquals(shardId, function.shardNum(hash));
                hash++;
            }
        }
    }

    public void testLegacySanity() {
        int routingNumShards = 1024;
        int routingFactor = randomFrom(512, 256, 128, 64, 32, 16, 8);
        var function = RoutingFunction.legacyRoutingNumberOfShards(routingNumShards, routingFactor);

        int iterations = randomIntBetween(1, 10);
        int hash = 0;
        for (int i = 1; i < iterations; i++) {
            for (int routingShardId = 0; routingShardId < routingNumShards; routingShardId++) {
                // We use modulo of routingNumShards and then divide by the routingFactor.
                // So we get the same destination shard every routingFactor iterations.
                int expected = routingShardId / routingFactor;
                assertEquals(expected, function.shardNum(hash));
                hash++;
            }
        }
    }
}
