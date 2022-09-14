/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class ClusterInfoSimulatorTests extends ESTestCase {

    public void testInitializeNewPrimary() {

        var newPrimary = TestShardRouting.newShardRouting("index-1", 0, "node-0", true, ShardRoutingState.INITIALIZING);

        var simulator = new ClusterInfoSimulator(
            new ClusterInfoTestBuilder().withNode("node-0", "/node-0/data", 1000, 1000)
                .withNode("node-1", "/node-1/data", 1000, 1000)
                .withShard(newPrimary, 0, "/node-0/data")
                .build()
        );
        simulator.simulate(newPrimary);

        assertThat(
            simulator.getClusterInfo(),
            equalTo(
                new ClusterInfoTestBuilder().withNode("node-0", "/node-0/data", 1000, 1000)
                    .withNode("node-1", "/node-1/data", 1000, 1000)
                    .withShard(newPrimary, 0, "/node-0/data")
                    .build()
            )
        );
    }

    public void testInitializeNewReplica() {

        var existingPrimary = TestShardRouting.newShardRouting("index-1", 0, "node-0", true, ShardRoutingState.STARTED);
        var newReplica = TestShardRouting.newShardRouting("index-1", 0, "node-1", false, ShardRoutingState.INITIALIZING);

        var simulator = new ClusterInfoSimulator(
            new ClusterInfoTestBuilder().withNode("node-0", "/node-0/data", 1000, 900)
                .withNode("node-1", "/node-1/data", 1000, 1000)
                .withShard(existingPrimary, 100, "/node-0/data")
                .withShard(newReplica, 0, "/node-1/data")
                .build()
        );
        simulator.simulate(newReplica);

        assertThat(
            simulator.getClusterInfo(),
            equalTo(
                new ClusterInfoTestBuilder().withNode("node-0", "/node-0/data", 1000, 900)
                    .withNode("node-1", "/node-1/data", 1000, 900)
                    .withShard(existingPrimary, 100, "/node-0/data")
                    .withShard(newReplica, 100, "/node-1/data")
                    .build()
            )
        );
    }

    public void testRelocateShard() {

        var shard = TestShardRouting.newShardRouting("index-1", 0, "node-1", "node-0", true, ShardRoutingState.INITIALIZING);

        var simulator = new ClusterInfoSimulator(
            new ClusterInfoTestBuilder().withNode("node-0", "/node-0/data", 1000, 1000)
                .withNode("node-1", "/node-1/data", 1000, 900)
                .withShard(shard, 100, "/node-1/data")
                .build()
        );
        simulator.simulate(shard);

        assertThat(
            simulator.getClusterInfo(),
            equalTo(
                new ClusterInfoTestBuilder().withNode("node-0", "/node-0/data", 1000, 900)
                    .withNode("node-1", "/node-1/data", 1000, 1000)
                    .withShard(shard, 100, "/node-0/data")
                    .build()
            )
        );
    }

    private static class ClusterInfoTestBuilder {

        private final Map<String, DiskUsage> diskUsage = new HashMap<>();
        private final Map<String, Long> shardSizes = new HashMap<>();
        private final Map<ShardRouting, String> routingToDataPath = new HashMap<>();

        public ClusterInfoTestBuilder withNode(String name, String dataPath, long total, long free) {
            diskUsage.put(name, new DiskUsage(name, name, dataPath, total, free));
            return this;
        }

        public ClusterInfoTestBuilder withShard(ShardRouting shard, long size, String dataPath) {
            shardSizes.put(ClusterInfo.shardIdentifierFromRouting(shard), size);
            routingToDataPath.put(shard, dataPath);
            return this;
        }

        public ClusterInfo build() {
            return new ClusterInfo(diskUsage, diskUsage, shardSizes, Map.of(), routingToDataPath, Map.of());
        }
    }
}
