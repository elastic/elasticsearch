/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterInfoSimulator;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.TestShardRouting.newShardRouting;
import static org.hamcrest.Matchers.equalTo;

public class ClusterInfoSimulatorTests extends ESTestCase {

    public void testInitializeNewPrimary() {

        var newPrimary = newShardRouting("index-1", 0, "node-0", true, INITIALIZING);

        var simulator = new ClusterInfoSimulator(
            new ClusterInfoTestBuilder() //
                .withNode("node-0", new DiskUsageBuilder(1000, 1000))
                .withNode("node-1", new DiskUsageBuilder(1000, 1000))
                .withShard(newPrimary, 0)
                .build()
        );
        simulator.simulate(newPrimary);

        assertThat(
            simulator.getClusterInfo(),
            equalTo(
                new ClusterInfoTestBuilder() //
                    .withNode("node-0", new DiskUsageBuilder(1000, 1000))
                    .withNode("node-1", new DiskUsageBuilder(1000, 1000))
                    .withShard(newPrimary, 0)
                    .build()
            )
        );
    }

    public void testInitializeNewReplica() {

        var existingPrimary = newShardRouting("index-1", 0, "node-0", true, STARTED);
        var newReplica = newShardRouting("index-1", 0, "node-1", false, INITIALIZING);

        var simulator = new ClusterInfoSimulator(
            new ClusterInfoTestBuilder() //
                .withNode("node-0", new DiskUsageBuilder(1000, 900))
                .withNode("node-1", new DiskUsageBuilder(1000, 1000))
                .withShard(existingPrimary, 100)
                .withShard(newReplica, 0)
                .build()
        );
        simulator.simulate(newReplica);

        assertThat(
            simulator.getClusterInfo(),
            equalTo(
                new ClusterInfoTestBuilder() //
                    .withNode("node-0", new DiskUsageBuilder(1000, 900))
                    .withNode("node-1", new DiskUsageBuilder(1000, 900))
                    .withShard(existingPrimary, 100)
                    .withShard(newReplica, 100)
                    .build()
            )
        );
    }

    public void testRelocateShard() {

        var fromNodeId = "node-0";
        var toNodeId = "node-1";

        var shard = newShardRouting("index-1", 0, toNodeId, fromNodeId, true, INITIALIZING);

        var simulator = new ClusterInfoSimulator(
            new ClusterInfoTestBuilder() //
                .withNode(fromNodeId, new DiskUsageBuilder(1000, 900))
                .withNode(toNodeId, new DiskUsageBuilder(1000, 1000))
                .withShard(shard, 100)
                .build()
        );
        simulator.simulate(shard);

        assertThat(
            simulator.getClusterInfo(),
            equalTo(
                new ClusterInfoTestBuilder() //
                    .withNode(fromNodeId, new DiskUsageBuilder(1000, 1000))
                    .withNode(toNodeId, new DiskUsageBuilder(1000, 900))
                    .withShard(shard, 100)
                    .build()
            )
        );
    }

    public void testRelocateShardWithMultipleDataPath1() {

        var fromNodeId = "node-0";
        var toNodeId = "node-1";

        var shard = newShardRouting("index-1", 0, toNodeId, fromNodeId, true, INITIALIZING);

        var simulator = new ClusterInfoSimulator(
            new ClusterInfoTestBuilder() //
                .withNode(fromNodeId, new DiskUsageBuilder("/data-1", 1000, 500), new DiskUsageBuilder("/data-2", 1000, 750))
                .withNode(toNodeId, new DiskUsageBuilder("/data-1", 1000, 750), new DiskUsageBuilder("/data-2", 1000, 900))
                .withShard(shard, 100)
                .build()
        );
        simulator.simulate(shard);

        assertThat(
            simulator.getClusterInfo(),
            equalTo(
                new ClusterInfoTestBuilder() //
                    .withNode(fromNodeId, new DiskUsageBuilder("/data-1", 1000, 500), new DiskUsageBuilder("/data-2", 1000, 850))
                    .withNode(toNodeId, new DiskUsageBuilder("/data-1", 1000, 650), new DiskUsageBuilder("/data-2", 1000, 900))
                    .withShard(shard, 100)
                    .build()
            )
        );
    }

    private static class ClusterInfoTestBuilder {

        private final Map<String, DiskUsage> leastAvailableSpaceUsage = new HashMap<>();
        private final Map<String, DiskUsage> mostAvailableSpaceUsage = new HashMap<>();
        private final Map<String, Long> shardSizes = new HashMap<>();

        public ClusterInfoTestBuilder withNode(String name, DiskUsageBuilder diskUsageBuilderBuilder) {
            leastAvailableSpaceUsage.put(name, diskUsageBuilderBuilder.toDiskUsage(name));
            mostAvailableSpaceUsage.put(name, diskUsageBuilderBuilder.toDiskUsage(name));
            return this;
        }

        public ClusterInfoTestBuilder withNode(String name, DiskUsageBuilder leastAvailableSpace, DiskUsageBuilder mostAvailableSpace) {
            leastAvailableSpaceUsage.put(name, leastAvailableSpace.toDiskUsage(name));
            mostAvailableSpaceUsage.put(name, mostAvailableSpace.toDiskUsage(name));
            return this;
        }

        public ClusterInfoTestBuilder withShard(ShardRouting shard, long size) {
            shardSizes.put(ClusterInfo.shardIdentifierFromRouting(shard), size);
            return this;
        }

        public ClusterInfo build() {
            return new ClusterInfo(leastAvailableSpaceUsage, mostAvailableSpaceUsage, shardSizes, Map.of(), Map.of(), Map.of());
        }
    }

    private record DiskUsageBuilder(String path, long total, long free) {

        private DiskUsageBuilder(long total, long free) {
            this("/data", total, free);
        }

        public DiskUsage toDiskUsage(String name) {
            return new DiskUsage(name, name, name + '/' + path, total, free);
        }
    }
}
