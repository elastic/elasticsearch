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

import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;

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

    public void testModuloHashForShard() {
        for (int iteration = 0; iteration < 20; iteration++) {
            int shards = between(1, 1024);
            var function = RoutingFunction.moduloNumberOfShards(shards);
            assertHashForShard(function, shards);

            int shardId = between(0, shards - 1);
            Set<Integer> shardsAfterSplit = new HashSet<>();
            for (int i = 0; i < 100; i++) {
                shardsAfterSplit.add(Math.floorMod(function.hashForShard(shardId, randomInt()), 2 * shards));
            }
            assertEquals(Set.of(shardId, shardId + shards), shardsAfterSplit);
        }
    }

    public void testLegacyHashForShard() {
        for (int iteration = 0; iteration < 20; iteration++) {
            int shards = between(1, 64);
            int routingFactor = between(1, 64);
            var function = RoutingFunction.legacyRoutingNumberOfShards(shards * routingFactor, routingFactor);
            assertHashForShard(function, shards);
        }
    }

    private static void assertHashForShard(RoutingFunction function, int shards) {
        for (int shardId = 0; shardId < shards; shardId++) {
            for (int random : new int[] { Integer.MIN_VALUE, -1, 0, Integer.MAX_VALUE, randomInt(), randomInt(), randomInt() }) {
                int hash = function.hashForShard(shardId, random);
                assertThat(hash, greaterThanOrEqualTo(0));
                assertEquals(shardId, function.shardNum(hash));
            }
        }
    }
}
