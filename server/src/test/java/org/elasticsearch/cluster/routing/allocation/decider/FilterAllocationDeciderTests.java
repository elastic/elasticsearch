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
package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.Decision.Type;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.gateway.TestGatewayAllocator;

import java.util.Arrays;
import java.util.Collections;

import static org.elasticsearch.cluster.metadata.IndexMetaData.INDEX_RESIZE_SOURCE_NAME;
import static org.elasticsearch.cluster.metadata.IndexMetaData.INDEX_RESIZE_SOURCE_UUID;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;

public class FilterAllocationDeciderTests extends ESAllocationTestCase {

    public void testFilterInitialRecovery() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        FilterAllocationDecider filterAllocationDecider = new FilterAllocationDecider(Settings.EMPTY, clusterSettings);
        AllocationDeciders allocationDeciders = new AllocationDeciders(
            Arrays.asList(filterAllocationDecider,
                new SameShardAllocationDecider(Settings.EMPTY, clusterSettings),
                new ReplicaAfterPrimaryActiveAllocationDecider()));
        AllocationService service = new AllocationService(allocationDeciders,
            new TestGatewayAllocator(), new BalancedShardsAllocator(Settings.EMPTY), EmptyClusterInfoService.INSTANCE);
        ClusterState state = createInitialClusterState(service, Settings.builder().put("index.routing.allocation.initial_recovery._id",
            "node2").build());
        RoutingTable routingTable = state.routingTable();

        // we can initially only allocate on node2
        assertEquals(routingTable.index("idx").shard(0).shards().get(0).state(), INITIALIZING);
        assertEquals(routingTable.index("idx").shard(0).shards().get(0).currentNodeId(), "node2");
        routingTable = service.applyFailedShard(state, routingTable.index("idx").shard(0).shards().get(0), randomBoolean()).routingTable();
        state = ClusterState.builder(state).routingTable(routingTable).build();
        assertEquals(routingTable.index("idx").shard(0).shards().get(0).state(), UNASSIGNED);
        assertNull(routingTable.index("idx").shard(0).shards().get(0).currentNodeId());

        // after failing the shard we are unassigned since the node is blacklisted and we can't initialize on the other node
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, state.getRoutingNodes(), state,
            null, 0);
        allocation.debugDecision(true);
        Decision.Single decision = (Decision.Single) filterAllocationDecider.canAllocate(
            routingTable.index("idx").shard(0).primaryShard(),
            state.getRoutingNodes().node("node2"), allocation);
        assertEquals(Type.YES, decision.type());
        assertEquals("node passes include/exclude/require filters", decision.getExplanation());
        ShardRouting primaryShard = routingTable.index("idx").shard(0).primaryShard();
        decision = (Decision.Single) filterAllocationDecider.canAllocate(
            routingTable.index("idx").shard(0).primaryShard(),
            state.getRoutingNodes().node("node1"), allocation);
        assertEquals(Type.NO, decision.type());
        if (primaryShard.recoverySource().getType() == RecoverySource.Type.LOCAL_SHARDS) {
            assertEquals("initial allocation of the shrunken index is only allowed on nodes [_id:\"node2\"] that " +
                         "hold a copy of every shard in the index", decision.getExplanation());
        } else {
            assertEquals("initial allocation of the index is only allowed on nodes [_id:\"node2\"]", decision.getExplanation());
        }

        state = service.reroute(state, "try allocate again");
        routingTable = state.routingTable();
        assertEquals(routingTable.index("idx").shard(0).primaryShard().state(), INITIALIZING);
        assertEquals(routingTable.index("idx").shard(0).primaryShard().currentNodeId(), "node2");

        state = startShardsAndReroute(service, state, routingTable.index("idx").shard(0).shardsWithState(INITIALIZING));
        routingTable = state.routingTable();

        // ok now we are started and can be allocated anywhere!! lets see...
        // first create another copy
        assertEquals(routingTable.index("idx").shard(0).replicaShards().get(0).state(), INITIALIZING);
        assertEquals(routingTable.index("idx").shard(0).replicaShards().get(0).currentNodeId(), "node1");
        state = startShardsAndReroute(service, state, routingTable.index("idx").shard(0).replicaShardsWithState(INITIALIZING));
        routingTable = state.routingTable();
        assertEquals(routingTable.index("idx").shard(0).replicaShards().get(0).state(), STARTED);
        assertEquals(routingTable.index("idx").shard(0).replicaShards().get(0).currentNodeId(), "node1");

        // now remove the node of the other copy and fail the current
        DiscoveryNode node1 = state.nodes().resolveNode("node1");
        state = service.disassociateDeadNodes(
            ClusterState.builder(state).nodes(DiscoveryNodes.builder(state.nodes()).remove("node1")).build(),
            true, "test");
        state = service.applyFailedShard(state, routingTable.index("idx").shard(0).primaryShard(), randomBoolean());

        // now bring back node1 and see it's assigned
        state = service.reroute(
            ClusterState.builder(state).nodes(DiscoveryNodes.builder(state.nodes()).add(node1)).build(), "test");
        routingTable = state.routingTable();
        assertEquals(routingTable.index("idx").shard(0).primaryShard().state(), INITIALIZING);
        assertEquals(routingTable.index("idx").shard(0).primaryShard().currentNodeId(), "node1");

        allocation = new RoutingAllocation(allocationDeciders, state.getRoutingNodes(), state,
            null, 0);
        allocation.debugDecision(true);
        decision = (Decision.Single) filterAllocationDecider.canAllocate(
            routingTable.index("idx").shard(0).shards().get(0),
            state.getRoutingNodes().node("node2"), allocation);
        assertEquals(Type.YES, decision.type());
        assertEquals("node passes include/exclude/require filters", decision.getExplanation());
        decision = (Decision.Single) filterAllocationDecider.canAllocate(
            routingTable.index("idx").shard(0).shards().get(0),
            state.getRoutingNodes().node("node1"), allocation);
        assertEquals(Type.YES, decision.type());
        assertEquals("node passes include/exclude/require filters", decision.getExplanation());
    }

    private ClusterState createInitialClusterState(AllocationService service, Settings settings) {
        MetaData.Builder metaData = MetaData.builder();
        final Settings.Builder indexSettings = settings(Version.CURRENT).put(settings);
        final IndexMetaData sourceIndex;
        //put a fake closed source index
        sourceIndex = IndexMetaData.builder("sourceIndex")
            .settings(settings(Version.CURRENT)).numberOfShards(2).numberOfReplicas(0)
            .putInSyncAllocationIds(0, Collections.singleton("aid0"))
            .putInSyncAllocationIds(1, Collections.singleton("aid1"))
            .build();
        metaData.put(sourceIndex, false);
        indexSettings.put(INDEX_RESIZE_SOURCE_UUID.getKey(), sourceIndex.getIndexUUID());
        indexSettings.put(INDEX_RESIZE_SOURCE_NAME.getKey(), sourceIndex.getIndex().getName());
        final IndexMetaData.Builder indexMetaDataBuilder = IndexMetaData.builder("idx").settings(indexSettings)
            .numberOfShards(1).numberOfReplicas(1);
        final IndexMetaData indexMetaData = indexMetaDataBuilder.build();
        metaData.put(indexMetaData, false);
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        routingTableBuilder.addAsFromCloseToOpen(sourceIndex);
        routingTableBuilder.addAsNew(indexMetaData);

        RoutingTable routingTable = routingTableBuilder.build();
        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING
            .getDefault(Settings.EMPTY)).metaData(metaData).routingTable(routingTable).build();
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();
        return service.reroute(clusterState, "reroute");
    }

    public void testInvalidIPFilter() {
        String ipKey = randomFrom("_ip", "_host_ip", "_publish_ip");
        Setting<String> filterSetting = randomFrom(IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING,
            IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_SETTING, IndexMetaData.INDEX_ROUTING_EXCLUDE_GROUP_SETTING);
        String invalidIP = randomFrom("192..168.1.1", "192.300.1.1");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            IndexScopedSettings indexScopedSettings = new IndexScopedSettings(Settings.EMPTY, IndexScopedSettings.BUILT_IN_INDEX_SETTINGS);
            indexScopedSettings.updateDynamicSettings(Settings.builder().put(filterSetting.getKey() + ipKey, invalidIP).build(),
                Settings.builder().put(Settings.EMPTY), Settings.builder(), "test ip validation");
        });
        assertEquals("invalid IP address [" + invalidIP + "] for [" + filterSetting.getKey() + ipKey + "]", e.getMessage());
    }

    public void testNull() {
        Setting<String> filterSetting = randomFrom(IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING,
            IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_SETTING, IndexMetaData.INDEX_ROUTING_EXCLUDE_GROUP_SETTING);

        IndexMetaData.builder("test")
            .settings(settings(Version.CURRENT).putNull(filterSetting.getKey() + "name")).numberOfShards(2).numberOfReplicas(0).build();
    }

    public void testWildcardIPFilter() {
        String ipKey = randomFrom("_ip", "_host_ip", "_publish_ip");
        Setting<String> filterSetting = randomFrom(IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING,
            IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_SETTING, IndexMetaData.INDEX_ROUTING_EXCLUDE_GROUP_SETTING);
        String wildcardIP = randomFrom("192.168.*", "192.*.1.1");
        IndexScopedSettings indexScopedSettings = new IndexScopedSettings(Settings.EMPTY, IndexScopedSettings.BUILT_IN_INDEX_SETTINGS);
        indexScopedSettings.updateDynamicSettings(Settings.builder().put(filterSetting.getKey() + ipKey, wildcardIP).build(),
            Settings.builder().put(Settings.EMPTY), Settings.builder(), "test ip validation");
    }
}
