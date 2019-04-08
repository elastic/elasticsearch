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
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.test.gateway.TestGatewayAllocator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING;
import static org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING;

public class EnableAllocationShortCircuitTests extends ESAllocationTestCase {

    public void testRebalancingSkippedIfDisabled() {
        AllocationService allocationService = createAllocationService(Settings.builder()
                .put(CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Allocation.NONE.name()),
            new RebalanceShortCircuitPlugin());

        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.get(Settings.EMPTY))
            .nodes(DiscoveryNodes.builder().add(newNode("node1"))).build();

        allocationService.reroute(clusterState, "reroute").routingTable();
    }

    public void testAllocationSkippedIfDisabled() {
        AllocationService allocationService = createAllocationService(Settings.builder()
                .put(CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), EnableAllocationDecider.Allocation.NONE.name()),
            new AllocateShortCircuitPlugin());

        MetaData metaData = MetaData.builder()
            .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0))
            .build();

        RoutingTable routingTable = RoutingTable.builder()
            .addAsNew(metaData.index("test"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metaData(metaData).routingTable(routingTable).nodes(DiscoveryNodes.builder().add(newNode("node1"))).build();

        allocationService.reroute(clusterState, "reroute").routingTable();
    }

    private AllocationService createAllocationService(Settings.Builder settings, ClusterPlugin plugin) {
        final ClusterSettings emptyClusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        List<AllocationDecider> deciders = new ArrayList<>(ClusterModule.createAllocationDeciders(settings.build(), emptyClusterSettings,
                Collections.singletonList(plugin)));
        return new MockAllocationService(
            new AllocationDeciders(deciders),
            new TestGatewayAllocator(), new BalancedShardsAllocator(Settings.EMPTY), EmptyClusterInfoService.INSTANCE);
    }

    private class RebalanceShortCircuitPlugin implements ClusterPlugin {
        @Override
        public Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {
            return Collections.singletonList(new RebalanceShortCircuitAllocationDecider());
        }

        private class RebalanceShortCircuitAllocationDecider extends AllocationDecider {
            @Override
            public Decision canRebalance(ShardRouting shardRouting, RoutingAllocation allocation) {
                throw new AssertionError("canRebalance was not bypassed");
            }

            @Override
            public Decision canRebalance(RoutingAllocation allocation) {
                throw new AssertionError("canRebalance was not bypassed");
            }
        }
    }

    private class AllocateShortCircuitPlugin implements ClusterPlugin {
        @Override
        public Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {
            return Collections.singletonList(new AllocateShortCircuitAllocationDecider());
        }

        private class AllocateShortCircuitAllocationDecider extends AllocationDecider {
            @Override
            public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                throw new AssertionError("canAllocate was not bypassed");
            }

            @Override
            public Decision canAllocate(ShardRouting shardRouting, RoutingAllocation allocation) {
                throw new AssertionError("canAllocate was not bypassed");
            }

            @Override
            public Decision canAllocate(IndexMetaData indexMetaData, RoutingNode node, RoutingAllocation allocation) {
                throw new AssertionError("canAllocate was not bypassed");
            }

            @Override
            public Decision canAllocate(RoutingNode node, RoutingAllocation allocation) {
                throw new AssertionError("canAllocate was not bypassed");
            }
        }
    }
}
