/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;

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
