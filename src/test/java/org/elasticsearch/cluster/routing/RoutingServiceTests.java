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

package org.elasticsearch.cluster.routing;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.FailedRerouteAllocation;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.StartedRerouteAllocation;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.GatewayAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocators;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.test.ElasticsearchAllocationTestCase;
import org.elasticsearch.test.cluster.TestClusterService;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.hamcrest.Matchers.equalTo;

/**
 */
public class RoutingServiceTests extends ElasticsearchAllocationTestCase {

    private TestRoutingService routingService;

    @Before
    public void createRoutingService() {
        routingService = new TestRoutingService();
    }

    @After
    public void shutdownRoutingService() throws Exception {
        routingService.shutdown();
    }

    @Test
    public void testReroute() {
        assertThat(routingService.hasReroutedAndClear(), equalTo(false));
        routingService.reroute("test");
        assertThat(routingService.hasReroutedAndClear(), equalTo(true));
    }

    @Test
    public void testNoDelayedUnassigned() throws Exception {
        AllocationService allocation = createAllocationService();
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(ImmutableSettings.builder().put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING, "0"))
                        .numberOfShards(1).numberOfReplicas(1))
                .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
                .metaData(metaData)
                .routingTable(RoutingTable.builder().addAsNew(metaData.index("test"))).build();
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().put(newNode("node1")).put(newNode("node2")).localNodeId("node1").masterNodeId("node1")).build();
        clusterState = ClusterState.builder(clusterState).routingResult(allocation.reroute(clusterState)).build();
        // starting primaries
        clusterState = ClusterState.builder(clusterState).routingResult(allocation.applyStartedShards(clusterState, clusterState.routingNodes().shardsWithState(INITIALIZING))).build();
        // starting replicas
        clusterState = ClusterState.builder(clusterState).routingResult(allocation.applyStartedShards(clusterState, clusterState.routingNodes().shardsWithState(INITIALIZING))).build();
        assertThat(clusterState.routingNodes().unassigned().size() > 0, equalTo(false));
        // remove node2 and reroute
        ClusterState prevState = clusterState;
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).remove("node2")).build();
        clusterState = ClusterState.builder(clusterState).routingResult(allocation.reroute(clusterState)).build();
        ClusterState newState = clusterState;

        assertThat(routingService.getRegisteredNextDelaySetting(), equalTo(Long.MAX_VALUE));
        routingService.clusterChanged(new ClusterChangedEvent("test", newState, prevState));
        assertThat(routingService.getRegisteredNextDelaySetting(), equalTo(Long.MAX_VALUE));
        assertThat(routingService.hasReroutedAndClear(), equalTo(false));
    }

    @Test
    @TestLogging("_root:DEBUG")
    public void testDelayedUnassignedScheduleReroute() throws Exception {
        AllocationService allocation = createAllocationService();
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(ImmutableSettings.builder().put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING, "100ms"))
                        .numberOfShards(1).numberOfReplicas(1))
                .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
                .metaData(metaData)
                .routingTable(RoutingTable.builder().addAsNew(metaData.index("test"))).build();
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().put(newNode("node1")).put(newNode("node2")).localNodeId("node1").masterNodeId("node1")).build();
        clusterState = ClusterState.builder(clusterState).routingResult(allocation.reroute(clusterState)).build();
        // starting primaries
        clusterState = ClusterState.builder(clusterState).routingResult(allocation.applyStartedShards(clusterState, clusterState.routingNodes().shardsWithState(INITIALIZING))).build();
        // starting replicas
        clusterState = ClusterState.builder(clusterState).routingResult(allocation.applyStartedShards(clusterState, clusterState.routingNodes().shardsWithState(INITIALIZING))).build();
        assertThat(clusterState.routingNodes().unassigned().size() > 0, equalTo(false));
        // remove node2 and reroute
        ClusterState prevState = clusterState;
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).remove("node2")).build();
        clusterState = ClusterState.builder(clusterState).routingResult(allocation.reroute(clusterState)).build();
        // We need to update the routing service's last attempted run to
        // signal that the GatewayAllocator tried to allocated it but
        // it was delayed
        routingService.setUnassignedShardsAllocatedTimestamp(System.currentTimeMillis());
        ClusterState newState = clusterState;

        routingService.clusterChanged(new ClusterChangedEvent("test", newState, prevState));
        assertBusy(new Runnable() {
            @Override
            public void run() {
                assertThat(routingService.hasReroutedAndClear(), equalTo(true));
            }
        });
        // verify the registration has been reset
        assertThat(routingService.getRegisteredNextDelaySetting(), equalTo(Long.MAX_VALUE));
    }

    /**
     * This tests that a new delayed reroute is scheduled right after a delayed reroute was run
     */
    public void testDelayedUnassignedScheduleRerouteAfterDelayedReroute() throws Exception {
        final ThreadPool testThreadPool = new ThreadPool(getTestName());

        try {
            DelayedShardsMockGatewayAllocator mockGatewayAllocator = new DelayedShardsMockGatewayAllocator();
            AllocationService allocation = new AllocationService(ImmutableSettings.EMPTY,
                    randomAllocationDeciders(ImmutableSettings.EMPTY, new NodeSettingsService(ImmutableSettings.EMPTY), getRandom()),
                    new ShardsAllocators(ImmutableSettings.EMPTY, mockGatewayAllocator, new BalancedShardsAllocator(ImmutableSettings.EMPTY)), EmptyClusterInfoService.getInstance());

            MetaData metaData = MetaData.builder()
                    .put(IndexMetaData.builder("short_delay").settings(ImmutableSettings.builder().put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING, "100ms"))
                            .numberOfShards(1).numberOfReplicas(1))
                    .put(IndexMetaData.builder("long_delay").settings(ImmutableSettings.builder().put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING, "10s"))
                            .numberOfShards(1).numberOfReplicas(1))
                    .build();
            ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metaData(metaData)
                    .routingTable(RoutingTable.builder().addAsNew(metaData.index("short_delay")).addAsNew(metaData.index("long_delay")).build())
                    .nodes(DiscoveryNodes.builder()
                    .put(newNode("node0", ImmutableMap.of("data", Boolean.FALSE.toString()))).localNodeId("node0").masterNodeId("node0")
                    .put(newNode("node1")).put(newNode("node2")).put(newNode("node3")).put(newNode("node4"))).build();
            // allocate shards
            clusterState = ClusterState.builder(clusterState).routingResult(allocation.reroute(clusterState)).build();
            // start primaries
            clusterState = ClusterState.builder(clusterState).routingResult(allocation.applyStartedShards(clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING))).build();
            // start replicas
            clusterState = ClusterState.builder(clusterState).routingResult(allocation.applyStartedShards(clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING))).build();
            assertThat("all shards should be started", clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(4));

            // find replica of short_delay
            ShardRouting shortDelayReplica = null;
            for (ShardRouting shardRouting : clusterState.getRoutingNodes().routingTable().allShards("short_delay")) {
                if (shardRouting.primary() == false) {
                    shortDelayReplica = shardRouting;
                    break;
                }
            }
            assertNotNull(shortDelayReplica);

            // find replica of long_delay
            ShardRouting longDelayReplica = null;
            for (ShardRouting shardRouting : clusterState.getRoutingNodes().routingTable().allShards("long_delay")) {
                if (shardRouting.primary() == false) {
                    longDelayReplica = shardRouting;
                    break;
                }
            }
            assertNotNull(longDelayReplica);

            // remove node of shortDelayReplica and node of longDelayReplica and reroute
            ClusterState prevState = clusterState;
            clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).remove(shortDelayReplica.currentNodeId()).remove(longDelayReplica.currentNodeId())).build();
            // make sure both replicas are marked as delayed (i.e. not reallocated)
            mockGatewayAllocator.setShardsToDelay(Arrays.asList(shortDelayReplica, longDelayReplica));
            clusterState = ClusterState.builder(clusterState).routingResult(allocation.reroute(clusterState)).build();

            // check that shortDelayReplica and longDelayReplica have been marked unassigned
            RoutingNodes.UnassignedShards unassigned = clusterState.getRoutingNodes().unassigned();
            assertEquals(2, unassigned.size());
            // update shortDelayReplica and longDelayReplica variables with new shard routing
            ShardRouting shortDelayUnassignedReplica = null;
            ShardRouting longDelayUnassignedReplica = null;
            for (ShardRouting shr : unassigned) {
                if (shr.getIndex().equals("short_delay")) {
                    shortDelayUnassignedReplica = shr;
                } else {
                    longDelayUnassignedReplica = shr;
                }
            }
            assertTrue(isSameShard(shortDelayReplica, shortDelayUnassignedReplica));
            assertTrue(isSameShard(longDelayReplica, longDelayUnassignedReplica));

            // manually trigger a clusterChanged event on routingService
            ClusterState newState = clusterState;
            // create fake cluster service
            TestClusterService clusterService = new TestClusterService(newState, testThreadPool);
            // create routing service, also registers listener on cluster service
            RoutingService routingService = new RoutingService(ImmutableSettings.EMPTY, testThreadPool, clusterService, allocation);
            routingService.start(); // just so performReroute does not prematurely return
            // ensure routing service has proper timestamp before triggering
            routingService.setUnassignedShardsAllocatedTimestamp(shortDelayUnassignedReplica.unassignedInfo().getTimestampInMillis() + randomIntBetween(0, 50));
            // next (delayed) reroute should only delay longDelayReplica/longDelayUnassignedReplica
            mockGatewayAllocator.setShardsToDelay(Arrays.asList(longDelayUnassignedReplica));
            // register listener on cluster state so we know when cluster state has been changed
            final CountDownLatch latch = new CountDownLatch(1);
            clusterService.addLast(new ClusterStateListener() {
                @Override
                public void clusterChanged(ClusterChangedEvent event) {
                    latch.countDown();
                }
            });
            // instead of clusterService calling clusterChanged, we call it directly here
            routingService.clusterChanged(new ClusterChangedEvent("test", newState, prevState));
             // cluster service should have updated state and called routingService with clusterChanged
            latch.await();
            // verify the registration has been set to the delay of longDelayReplica/longDelayUnassignedReplica
            assertThat(routingService.getRegisteredNextDelaySetting(), equalTo(10000L));
        } finally {
            terminate(testThreadPool);
        }
    }

    public static boolean isSameShard(ShardRouting shard1, ShardRouting shard2) {
        return shard1.getIndex().equals(shard2.getIndex()) && shard1.getId() == shard2.getId();
    }

    @Test
    public void testDelayedUnassignedDoesNotRerouteForNegativeDelays() throws Exception {
        AllocationService allocation = createAllocationService();
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(ImmutableSettings.builder().put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING, "100ms"))
                        .numberOfShards(1).numberOfReplicas(1))
                .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
                .metaData(metaData)
                .routingTable(RoutingTable.builder().addAsNew(metaData.index("test"))).build();
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().put(newNode("node1")).put(newNode("node2")).localNodeId("node1").masterNodeId("node1")).build();
        clusterState = ClusterState.builder(clusterState).routingResult(allocation.reroute(clusterState)).build();
        // starting primaries
        clusterState = ClusterState.builder(clusterState).routingResult(allocation.applyStartedShards(clusterState, clusterState.routingNodes().shardsWithState(INITIALIZING))).build();
        // starting replicas
        clusterState = ClusterState.builder(clusterState).routingResult(allocation.applyStartedShards(clusterState, clusterState.routingNodes().shardsWithState(INITIALIZING))).build();
        assertEquals(clusterState.routingNodes().unassigned().size(), 0);
        // remove node2 and reroute
        ClusterState prevState = clusterState;
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).remove("node2")).build();
        clusterState = ClusterState.builder(clusterState).routingResult(allocation.reroute(clusterState)).build();
        // Set it in the future so the delay will be negative
        routingService.setUnassignedShardsAllocatedTimestamp(System.currentTimeMillis() + TimeValue.timeValueMinutes(1).millis());

        ClusterState newState = clusterState;

        routingService.clusterChanged(new ClusterChangedEvent("test", newState, prevState));
        assertBusy(new Runnable() {
            @Override
            public void run() {
                assertThat(routingService.hasReroutedAndClear(), equalTo(false));

                // verify the registration has been updated
                assertThat(routingService.getRegisteredNextDelaySetting(), equalTo(100L));
            }
        });
    }

    private class TestRoutingService extends RoutingService {

        private AtomicBoolean rerouted = new AtomicBoolean();

        public TestRoutingService() {
            super(ImmutableSettings.EMPTY, new ThreadPool(getTestName()), null, null);
        }

        void shutdown() throws Exception {
            terminate(threadPool);
        }

        public boolean hasReroutedAndClear() {
            return rerouted.getAndSet(false);
        }

        @Override
        void performReroute(String reason) {
            rerouted.set(true);
        }
    }

    /**
     * Mocks behavior in LocalGatewayAllocator to remove delayed shards from list of unassigned shards so they don't get reassigned yet.
     * It does not implement the full logic but shards that are to be delayed need to be explicitly set using the method setShardsToDelay(...).
     */
    private static class DelayedShardsMockGatewayAllocator implements GatewayAllocator {
        volatile List<ShardRouting> delayedShards = Collections.emptyList();

        @Override
        public void applyStartedShards(StartedRerouteAllocation allocation) {}

        @Override
        public void applyFailedShards(FailedRerouteAllocation allocation) {}

        /**
         * Explicitly set which shards should be delayed in the next allocateUnassigned calls
         */
        public void setShardsToDelay(List<ShardRouting> delayedShards) {
            this.delayedShards = delayedShards;
        }

        @Override
        public boolean allocateUnassigned(RoutingAllocation allocation) {
            final RoutingNodes routingNodes = allocation.routingNodes();
            final RoutingNodes.UnassignedShards.UnassignedIterator unassignedIterator = routingNodes.unassigned().iterator();
            boolean changed = false;
            while (unassignedIterator.hasNext()) {
                MutableShardRouting shard = unassignedIterator.next();
                for (ShardRouting shardToDelay : delayedShards) {
                    if (isSameShard(shard, shardToDelay)) {
                        changed = true;
                        unassignedIterator.removeAndIgnore();
                    }
                }
            }
            return changed;
        }
    }
}
