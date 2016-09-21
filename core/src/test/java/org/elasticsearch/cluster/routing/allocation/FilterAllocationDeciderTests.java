/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ReplicaAfterPrimaryActiveAllocationDecider;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.gateway.NoopGatewayAllocator;

import java.util.Arrays;

import static org.elasticsearch.cluster.metadata.IndexMetaData.INDEX_SHRINK_SOURCE_NAME;
import static org.elasticsearch.cluster.metadata.IndexMetaData.INDEX_SHRINK_SOURCE_UUID;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;

public class FilterAllocationDeciderTests extends ESAllocationTestCase {

    public void testFilterInitialRecovery() {
        FilterAllocationDecider filterAllocationDecider = new FilterAllocationDecider(Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
        AllocationDeciders allocationDeciders = new AllocationDeciders(Settings.EMPTY,
            Arrays.asList(filterAllocationDecider, new ReplicaAfterPrimaryActiveAllocationDecider(Settings.EMPTY)));
        AllocationService service = new AllocationService(Settings.builder().build(), allocationDeciders,
            NoopGatewayAllocator.INSTANCE, new BalancedShardsAllocator(Settings.EMPTY), EmptyClusterInfoService.INSTANCE);
        ClusterState state = createInitialClusterState(service, Settings.builder().put("index.routing.allocation.initial_recovery._id",
            "node2").build());
        RoutingTable routingTable = state.routingTable();

        // we can initally only allocate on node2
        assertEquals(routingTable.index("idx").shard(0).shards().get(0).state(), INITIALIZING);
        assertEquals(routingTable.index("idx").shard(0).shards().get(0).currentNodeId(), "node2");
        routingTable = service.applyFailedShard(state, routingTable.index("idx").shard(0).shards().get(0)).routingTable();
        state = ClusterState.builder(state).routingTable(routingTable).build();
        assertEquals(routingTable.index("idx").shard(0).shards().get(0).state(), UNASSIGNED);
        assertNull(routingTable.index("idx").shard(0).shards().get(0).currentNodeId());

        // after failing the shard we are unassigned since the node is blacklisted and we can't initialize on the other node
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, state.getRoutingNodes(), state,
            null, 0, false);
        assertEquals(filterAllocationDecider.canAllocate(routingTable.index("idx").shard(0).primaryShard(),
            state.getRoutingNodes().node("node2")
            ,allocation), Decision.YES);
        assertEquals(filterAllocationDecider.canAllocate(routingTable.index("idx").shard(0).primaryShard(),
            state.getRoutingNodes().node("node1")
            ,allocation), Decision.NO);

        state = service.reroute(state, "try allocate again");
        routingTable = state.routingTable();
        assertEquals(routingTable.index("idx").shard(0).primaryShard().state(), INITIALIZING);
        assertEquals(routingTable.index("idx").shard(0).primaryShard().currentNodeId(), "node2");

        state = service.applyStartedShards(state, routingTable.index("idx").shard(0).shardsWithState(INITIALIZING));
        routingTable = state.routingTable();

        // ok now we are started and can be allocated anywhere!! lets see...
        assertEquals(routingTable.index("idx").shard(0).primaryShard().state(), STARTED);
        assertEquals(routingTable.index("idx").shard(0).primaryShard().currentNodeId(), "node2");

        // replicas should be initializing
        assertEquals(routingTable.index("idx").shard(0).replicaShards().get(0).state(), INITIALIZING);
        assertEquals(routingTable.index("idx").shard(0).replicaShards().get(0).currentNodeId(), "node1");

        // we fail it again to check if we are initializing immediately on the other node
        state = service.applyFailedShard(state, routingTable.index("idx").shard(0).shards().get(0));
        routingTable = state.routingTable();
        assertEquals(routingTable.index("idx").shard(0).shards().get(0).state(), INITIALIZING);
        assertEquals(routingTable.index("idx").shard(0).shards().get(0).currentNodeId(), "node1");

        allocation = new RoutingAllocation(allocationDeciders, state.getRoutingNodes(), state,
            null, 0, false);
        assertEquals(filterAllocationDecider.canAllocate(routingTable.index("idx").shard(0).shards().get(0),
            state.getRoutingNodes().node("node2")
            ,allocation), Decision.YES);
        assertEquals(filterAllocationDecider.canAllocate(routingTable.index("idx").shard(0).shards().get(0),
            state.getRoutingNodes().node("node1")
            ,allocation), Decision.YES);
    }

    private ClusterState createInitialClusterState(AllocationService service, Settings settings) {
        boolean shrinkIndex = randomBoolean();
        MetaData.Builder metaData = MetaData.builder();
        final Settings.Builder indexSettings = settings(Version.CURRENT).put(settings);
        final IndexMetaData sourceIndex;
        if (shrinkIndex) {
            //put a fake closed source index
            sourceIndex = IndexMetaData.builder("sourceIndex")
                .settings(settings(Version.CURRENT)).numberOfShards(2).numberOfReplicas(0).build();
            metaData.put(sourceIndex, false);
            indexSettings.put(INDEX_SHRINK_SOURCE_UUID.getKey(), sourceIndex.getIndexUUID());
            indexSettings.put(INDEX_SHRINK_SOURCE_NAME.getKey(), sourceIndex.getIndex().getName());
        } else {
            sourceIndex = null;
        }
        final IndexMetaData indexMetaData = IndexMetaData.builder("idx").settings(indexSettings)
            .numberOfShards(1).numberOfReplicas(1).build();
        metaData.put(indexMetaData, false);
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        if (shrinkIndex) {
            routingTableBuilder.addAsFromCloseToOpen(sourceIndex);
            routingTableBuilder.addAsNew(indexMetaData);
        } if (randomBoolean()) {
            routingTableBuilder.addAsNew(indexMetaData);
        } else {
            routingTableBuilder.addAsRestore(indexMetaData, new RecoverySource.SnapshotRecoverySource(
                new Snapshot("repository", new SnapshotId("snapshot_name", "snapshot_uuid")),
                Version.CURRENT, indexMetaData.getIndex().getName()));
        }

        RoutingTable routingTable = routingTableBuilder.build();
        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING
            .getDefault(Settings.EMPTY)).metaData(metaData).routingTable(routingTable).build();
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();
        return service.reroute(clusterState, "reroute", false);
    }
}
