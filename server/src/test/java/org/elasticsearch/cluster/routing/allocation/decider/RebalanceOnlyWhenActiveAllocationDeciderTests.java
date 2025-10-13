/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.elasticsearch.cluster.routing.TestShardRouting.newShardRouting;
import static org.elasticsearch.cluster.routing.allocation.decider.RebalanceOnlyWhenActiveAllocationDecider.NO_SOME_REPLICAS_INACTIVE;
import static org.elasticsearch.cluster.routing.allocation.decider.RebalanceOnlyWhenActiveAllocationDecider.YES_ALL_REPLICAS_ACTIVE;
import static org.hamcrest.Matchers.equalTo;

public class RebalanceOnlyWhenActiveAllocationDeciderTests extends ESAllocationTestCase {

    public void testAllowRebalanceWhenAllShardsActive() {

        var index = new Index("test", "_na_");
        var primary = newShardRouting(new ShardId(index, 0), "node-1", true, STARTED);
        var replica = newShardRouting(new ShardId(index, 0), "node-2", false, STARTED);

        var state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(IndexMetadata.builder(index.getName()).settings(indexSettings(IndexVersion.current(), 1, 1))))
            .nodes(DiscoveryNodes.builder().add(newNode("node-1")).add(newNode("node-2")).add(newNode("node-3")))
            .routingTable(RoutingTable.builder().add(IndexRoutingTable.builder(index).addShard(primary).addShard(replica)))
            .build();

        var allocation = createRoutingAllocation(state);
        var decider = new RebalanceOnlyWhenActiveAllocationDecider();

        assertThat(decider.canRebalance(primary, allocation), equalTo(YES_ALL_REPLICAS_ACTIVE));
        assertThat(decider.canRebalance(replica, allocation), equalTo(YES_ALL_REPLICAS_ACTIVE));
    }

    public void testAllowRebalanceForMultipleIndicesAcrossMultipleProjects() {
        final List<ShardRouting> shards = new ArrayList<>();

        final List<String> nodeIds = randomList(3, 10, () -> randomAlphaOfLengthBetween(3, 8));
        final GlobalRoutingTable.Builder routingTable = GlobalRoutingTable.builder();
        final Metadata.Builder metadata = Metadata.builder();

        final int numberOfProjects = randomIntBetween(2, 5);
        final Iterator<String> nodeItr = Iterators.cycling(nodeIds);
        for (int p = 1; p <= numberOfProjects; p++) {
            final int numberOfIndices = randomIntBetween(1, 3);
            var project = ProjectMetadata.builder(randomUniqueProjectId());
            var rt = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY);
            for (int i = 1; i <= numberOfIndices; i++) {
                final int numberOfShards = randomIntBetween(1, 5);
                final int numberOfReplicas = randomIntBetween(0, 2);
                var index = new Index("index-" + i, randomUUID());
                var irt = IndexRoutingTable.builder(index);
                for (int s = 0; s < numberOfShards; s++) {
                    final ShardId shard = new ShardId(index, s);
                    var primary = newShardRouting(shard, nodeItr.next(), true, STARTED);
                    irt.addShard(primary);
                    shards.add(primary);
                    for (int r = 0; r < numberOfReplicas; r++) {
                        var replica = newShardRouting(shard, nodeItr.next(), false, STARTED);
                        irt.addShard(replica);
                        shards.add(replica);
                    }
                }
                project.put(
                    IndexMetadata.builder(index.getName())
                        .settings(
                            indexSettings(IndexVersion.current(), numberOfShards, numberOfReplicas).put(
                                IndexMetadata.SETTING_INDEX_UUID,
                                index.getUUID()
                            )
                        )
                );
                rt.add(irt);
            }
            metadata.put(project);
            routingTable.put(project.getId(), rt);
        }

        assertThat(shards.size(), Matchers.greaterThanOrEqualTo(numberOfProjects));

        var nodes = DiscoveryNodes.builder();
        nodeIds.forEach(id -> nodes.add(newNode(id)));
        var state = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).nodes(nodes).routingTable(routingTable.build()).build();

        var allocation = createRoutingAllocation(state);
        var decider = new RebalanceOnlyWhenActiveAllocationDecider();

        for (var shard : shards) {
            assertThat(decider.canRebalance(shard, allocation), equalTo(YES_ALL_REPLICAS_ACTIVE));
        }
    }

    public void testDoNotAllowRebalanceWhenSomeShardsAreNotActive() {

        var index = new Index("test", "_na_");
        var primary = newShardRouting(new ShardId(index, 0), "node-1", true, STARTED);
        var replica = randomBoolean()
            ? newShardRouting(new ShardId(index, 0), null, false, UNASSIGNED)
            : newShardRouting(new ShardId(index, 0), "node-2", false, INITIALIZING);

        var state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(IndexMetadata.builder(index.getName()).settings(indexSettings(IndexVersion.current(), 1, 1))))
            .nodes(DiscoveryNodes.builder().add(newNode("node-1")).add(newNode("node-2")).add(newNode("node-3")))
            .routingTable(RoutingTable.builder().add(IndexRoutingTable.builder(index).addShard(primary).addShard(replica)))
            .build();

        var allocation = createRoutingAllocation(state);
        var decider = new RebalanceOnlyWhenActiveAllocationDecider();

        assertThat(decider.canRebalance(primary, allocation), equalTo(NO_SOME_REPLICAS_INACTIVE));
    }

    public void testDoNotAllowRebalanceWhenSomeShardsAreNotActiveAndRebalancing() {

        var index = new Index("test", "_na_");
        var primary = newShardRouting(new ShardId(index, 0), "node-1", true, STARTED);
        var replica1 = newShardRouting(new ShardId(index, 0), "node-2", "node-3", false, RELOCATING);
        var replica2 = newShardRouting(new ShardId(index, 0), null, false, UNASSIGNED);

        var state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(IndexMetadata.builder(index.getName()).settings(indexSettings(IndexVersion.current(), 1, 2))))
            .nodes(DiscoveryNodes.builder().add(newNode("node-1")).add(newNode("node-2")).add(newNode("node-3")))
            .routingTable(
                RoutingTable.builder().add(IndexRoutingTable.builder(index).addShard(primary).addShard(replica1).addShard(replica2))
            )
            .build();

        var allocation = createRoutingAllocation(state);
        var decider = new RebalanceOnlyWhenActiveAllocationDecider();

        assertThat(decider.canRebalance(primary, allocation), equalTo(NO_SOME_REPLICAS_INACTIVE));
    }

    public void testAllowConcurrentRebalance() {

        var index = new Index("test", "_na_");
        var primary = newShardRouting(new ShardId(index, 0), "node-1", true, STARTED);
        var replica = newShardRouting(new ShardId(index, 0), "node-2", "node-3", false, RELOCATING);

        var state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(IndexMetadata.builder(index.getName()).settings(indexSettings(IndexVersion.current(), 1, 1))))
            .nodes(DiscoveryNodes.builder().add(newNode("node-1")).add(newNode("node-2")).add(newNode("node-3")))
            .routingTable(RoutingTable.builder().add(IndexRoutingTable.builder(index).addShard(primary).addShard(replica)))
            .build();

        var allocation = createRoutingAllocation(state);
        var decider = new RebalanceOnlyWhenActiveAllocationDecider();

        assertThat(decider.canRebalance(primary, allocation), equalTo(YES_ALL_REPLICAS_ACTIVE));
    }

    private static RoutingAllocation createRoutingAllocation(ClusterState state) {
        return new RoutingAllocation(new AllocationDeciders(List.of()), state, null, null, 0L);
    }
}
