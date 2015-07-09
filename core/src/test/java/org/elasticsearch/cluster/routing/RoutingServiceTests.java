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

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchAllocationTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT).put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING, "0"))
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
        assertThat(clusterState.routingNodes().hasUnassigned(), equalTo(false));
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
    public void testDelayedUnassignedScheduleReroute() throws Exception {
        AllocationService allocation = createAllocationService();
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT).put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING, "100ms"))
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
        assertThat(clusterState.routingNodes().hasUnassigned(), equalTo(false));
        // remove node2 and reroute
        ClusterState prevState = clusterState;
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).remove("node2")).build();
        clusterState = ClusterState.builder(clusterState).routingResult(allocation.reroute(clusterState)).build();
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

    private class TestRoutingService extends RoutingService {

        private AtomicBoolean rerouted = new AtomicBoolean();

        public TestRoutingService() {
            super(Settings.EMPTY, new ThreadPool(getTestName()), null, null);
        }

        void shutdown() throws Exception {
            terminate(threadPool);
        }

        public boolean hasReroutedAndClear() {
            return rerouted.getAndSet(false);
        }

        @Override
        protected void performReroute(String reason) {
            rerouted.set(true);
        }
    }
}
