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
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.allocator.DelayUnassignedAllocation;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.gateway.GatewayAllocator;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.test.ElasticsearchAllocationTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;

/**
 */
public class DelayUnassignedAllocationTests extends ElasticsearchAllocationTestCase {

    @Test
    public void testDelayNodeComesBackWithinWindow() throws Exception {
        TestDelayedAllocator allocator = new TestDelayedAllocator();
        AllocationService allocationService = delayedAllocationService(allocator);

        logger.info("build a 1/1 index with 2 node cluster");
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .build();
        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable).build();
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().put(newNode("node1")).put(newNode("node2"))).build();

        logger.info("reroute till all shards are started");
        clusterState = ClusterState.builder(clusterState).routingResult(allocationService.reroute(clusterState)).build();
        assertThat(allocator.lastResult.isRerouteRequired(), equalTo(false));
        clusterState = ClusterState.builder(clusterState).routingResult(allocationService.applyStartedShards(clusterState, clusterState.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING))).build();
        assertThat(allocator.lastResult.isRerouteRequired(), equalTo(false));
        clusterState = ClusterState.builder(clusterState).routingResult(allocationService.applyStartedShards(clusterState, clusterState.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING))).build();
        assertThat(allocator.lastResult.isRerouteRequired(), equalTo(false));
        assertThat(clusterState.routingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(2));
        assertThat(allocator.allocation.getDelayedNodes().size(), equalTo(0));
        assertThat(allocator.allocation.getNumberOfDelayedShards(), equalTo(0));

        allocator.allocation.setDuration(TimeValue.timeValueHours(1));

        logger.info("remove node2, verify allocation is delayed");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).remove("node2")).build();

        clusterState = ClusterState.builder(clusterState).routingResult(allocationService.reroute(clusterState)).build();
        assertThat(clusterState.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED).size(), equalTo(1));
        assertThat(allocator.allocation.getDelayedNodes().size(), equalTo(1));
        assertThat(allocator.allocation.getNumberOfDelayedShards(), equalTo(1));
        assertThat(allocator.lastResult.isRerouteRequired(), equalTo(true));
        assertThat(allocator.lastResult.getRerouteSchedule(), equalTo(TimeValue.timeValueHours(1)));

        logger.info("another reroute should not cause another scheduled reroute, once is enoguh");
        clusterState = ClusterState.builder(clusterState).routingResult(allocationService.reroute(clusterState)).build();
        assertThat(allocator.allocation.getDelayedNodes().size(), equalTo(1));
        assertThat(allocator.allocation.getNumberOfDelayedShards(), equalTo(1));
        assertThat(clusterState.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED).size(), equalTo(1));
        assertThat(allocator.lastResult.isRerouteRequired(), equalTo(false));


        logger.info("bring back node2, see that it gets allocated to");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).put(newNode("node2"))).build();
        clusterState = ClusterState.builder(clusterState).routingResult(allocationService.reroute(clusterState)).build();
        assertThat(allocator.allocation.getDelayedNodes().size(), equalTo(0));
        assertThat(allocator.allocation.getNumberOfDelayedShards(), equalTo(0));
        assertThat(clusterState.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED).size(), equalTo(0));
    }

    @Test
    public void testDelayNodeDoesntComeBackWithinWindow() throws Exception {
        TestDelayedAllocator allocator = new TestDelayedAllocator();
        AllocationService allocationService = delayedAllocationService(allocator);

        logger.info("build a 1/1 index with 2 node cluster");
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .build();
        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable).build();
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().put(newNode("node1")).put(newNode("node2"))).build();

        logger.info("reroute till all shards are started");
        clusterState = ClusterState.builder(clusterState).routingResult(allocationService.reroute(clusterState)).build();
        assertThat(allocator.lastResult.isRerouteRequired(), equalTo(false));
        clusterState = ClusterState.builder(clusterState).routingResult(allocationService.applyStartedShards(clusterState, clusterState.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING))).build();
        assertThat(allocator.lastResult.isRerouteRequired(), equalTo(false));
        clusterState = ClusterState.builder(clusterState).routingResult(allocationService.applyStartedShards(clusterState, clusterState.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING))).build();
        assertThat(allocator.lastResult.isRerouteRequired(), equalTo(false));
        assertThat(clusterState.routingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(2));
        assertThat(allocator.allocation.getDelayedNodes().size(), equalTo(0));
        assertThat(allocator.allocation.getNumberOfDelayedShards(), equalTo(0));

        allocator.allocation.setDuration(TimeValue.timeValueHours(1));

        logger.info("remove node2, verify allocation is delayed");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).remove("node2")).build();
        clusterState = ClusterState.builder(clusterState).routingResult(allocationService.reroute(clusterState)).build();

        assertThat(clusterState.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED).size(), equalTo(1));
        assertThat(allocator.allocation.getDelayedNodes().size(), equalTo(1));
        assertThat(allocator.allocation.getNumberOfDelayedShards(), equalTo(1));
        assertThat(allocator.lastResult.isRerouteRequired(), equalTo(true));
        assertThat(allocator.lastResult.getRerouteSchedule(), equalTo(TimeValue.timeValueHours(1)));

        logger.info("reduce duration to 1ms, verify that nothing is assigned still, and no reroute is scheduled");
        allocator.allocation.setDuration(TimeValue.timeValueMillis(1));
        Thread.sleep(10);
        clusterState = ClusterState.builder(clusterState).routingResult(allocationService.reroute(clusterState)).build();
        assertThat(allocator.allocation.getDelayedNodes().size(), equalTo(0));
        assertThat(allocator.allocation.getNumberOfDelayedShards(), equalTo(0));
        assertThat(allocator.lastResult.isRerouteRequired(), equalTo(false));
        assertThat(clusterState.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED).size(), equalTo(1));
    }

    @Test
    public void testAllNodesGoAwayNodeHoldingReplicaComesBack() {
        TestDelayedAllocator allocator = new TestDelayedAllocator();
        AllocationService allocationService = delayedAllocationService(allocator);

        logger.info("build a 1/1 index with 2 node cluster, verified primary exists on node1");
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .build();
        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable).build();
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().put(newNode("node1"))).build();

        clusterState = ClusterState.builder(clusterState).routingResult(allocationService.reroute(clusterState)).build();
        clusterState = ClusterState.builder(clusterState).routingResult(allocationService.applyStartedShards(clusterState, clusterState.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING))).build();

        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).put(newNode("node2"))).build();
        clusterState = ClusterState.builder(clusterState).routingResult(allocationService.reroute(clusterState)).build();
        clusterState = ClusterState.builder(clusterState).routingResult(allocationService.applyStartedShards(clusterState, clusterState.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING))).build();

        assertThat(clusterState.routingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(2));
        assertThat(allocator.allocation.getDelayedNodes().size(), equalTo(0));
        assertThat(allocator.allocation.getNumberOfDelayedShards(), equalTo(0));

        logger.info("kill both nodes");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).remove("node1").remove("node2")).build();
        clusterState = ClusterState.builder(clusterState).routingResult(allocationService.reroute(clusterState)).build();
        assertThat(clusterState.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED).size(), equalTo(2));
        assertThat(allocator.allocation.getDelayedNodes().size(), equalTo(2));
        assertThat(allocator.allocation.getNumberOfDelayedShards(), equalTo(2));

        logger.info("bring back node2 holding the replica, verify primary is allocated");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).put(newNode("node2"))).build();
        clusterState = ClusterState.builder(clusterState).routingResult(allocationService.reroute(clusterState)).build();
        assertThat(clusterState.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED).size(), equalTo(1));
        assertThat(clusterState.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(1));
        assertThat(allocator.allocation.getDelayedNodes().size(), equalTo(1));
        assertThat(allocator.allocation.getNumberOfDelayedShards(), equalTo(1));
    }

    private AllocationService delayedAllocationService(TestDelayedAllocator allocator) {
        return createAllocationService(Settings.EMPTY, new NodeSettingsService(Settings.EMPTY), allocator, getRandom());
    }

    static class TestDelayedAllocator extends GatewayAllocator {

        public final DelayUnassignedAllocation allocation;
        public DelayUnassignedAllocation.Result lastResult;

        public TestDelayedAllocator() {
            this(Settings.EMPTY);
        }

        public TestDelayedAllocator(Settings settings) {
            super(settings, null, new NodeSettingsService(Settings.EMPTY), null, null);
            if (settings.get(DelayUnassignedAllocation.DELAY_ALLOCATION_NODE_KEY) == null) {
                settings = Settings.builder().put(settings).put(DelayUnassignedAllocation.DELAY_ALLOCATION_NODE_KEY, DelayUnassignedAllocation.NodeKey.ID).build();
            }
            this.allocation = new DelayUnassignedAllocation(settings);
        }

        @Override
        public void applyStartedShards(StartedRerouteAllocation allocation) {
            // ignore
        }

        @Override
        public void applyFailedShards(FailedRerouteAllocation allocation) {
            // ignore
        }

        @Override
        public boolean allocateUnassigned(RoutingAllocation allocation) {
            this.lastResult = this.allocation.delayUnassignedAllocation(allocation);
            return false;
        }
    }
}
