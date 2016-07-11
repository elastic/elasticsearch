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

package org.elasticsearch.discovery.zen;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.LocalTransportAddress;
import org.elasticsearch.discovery.zen.elect.ElectMasterService;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.CoreMatchers.equalTo;

public class NodeRemovalClusterStateTaskExecutorTests extends ESTestCase {

    public void testRemovingNonExistentNodes() throws Exception {
        final ZenDiscovery.NodeRemovalClusterStateTaskExecutor executor =
                new ZenDiscovery.NodeRemovalClusterStateTaskExecutor(null, null, null, logger);
        final DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        final int nodes = randomIntBetween(2, 16);
        for (int i = 0; i < nodes; i++) {
            builder.put(node(i));
        }
        final ClusterState clusterState = ClusterState.builder(new ClusterName("test")).nodes(builder).build();

        final DiscoveryNodes.Builder removeBuilder = DiscoveryNodes.builder();
        for (int i = nodes; i < nodes + randomIntBetween(1, 16); i++) {
            removeBuilder.put(node(i));
        }
        final List<ZenDiscovery.NodeRemovalClusterStateTaskExecutor.Task> tasks = new ArrayList<>();
        for (final DiscoveryNode node : removeBuilder.build()) {
            tasks.add(new ZenDiscovery.NodeRemovalClusterStateTaskExecutor.Task(node, randomBoolean() ? "left" : "failed"));
        }

        final ClusterStateTaskExecutor.BatchResult<ZenDiscovery.NodeRemovalClusterStateTaskExecutor.Task> result
                = executor.execute(clusterState, tasks);
        assertThat(result.resultingState, equalTo(clusterState));
    }

    public void testNotEnoughMasterNodesAfterRemove() throws Exception {
        final AtomicReference<Iterable<DiscoveryNode>> capturedNodes = new AtomicReference<>();
        final ElectMasterService electMasterService = new ElectMasterService(Settings.EMPTY, Version.CURRENT) {
            @Override
            public boolean hasEnoughMasterNodes(Iterable<DiscoveryNode> nodes) {
                capturedNodes.set(nodes);
                return false;
            }
        };

        final AllocationService allocationService = new AllocationService(Settings.EMPTY, null, null, null) {
            @Override
            public RoutingAllocation.Result reroute(ClusterState clusterState, String reason) {
                fail("reroute should not be invoked when not enough master nodes");
                return new RoutingAllocation.Result(randomBoolean(), null);
            }
        };

        final AtomicBoolean rejoined = new AtomicBoolean();
        final AtomicReference<ClusterState> rejoinedClusterState = new AtomicReference<>();
        final ZenDiscovery.NodeRemovalClusterStateTaskExecutor.Rejoin rejoin = new ZenDiscovery.NodeRemovalClusterStateTaskExecutor.Rejoin() {
            @Override
            public ClusterState apply(ClusterState clusterState, String reason) {
                rejoined.set(true);
                rejoinedClusterState.set(ClusterState.builder(clusterState).build());
                return rejoinedClusterState.get();
            }
        };

        final AtomicReference<ClusterState> remainingNodesClusterState = new AtomicReference<>();
        final ZenDiscovery.NodeRemovalClusterStateTaskExecutor executor =
                new ZenDiscovery.NodeRemovalClusterStateTaskExecutor(allocationService, electMasterService, rejoin, logger) {
                    @Override
                    ClusterState remainingNodesClusterState(ClusterState currentState, DiscoveryNodes.Builder remainingNodesBuilder) {
                        remainingNodesClusterState.set(super.remainingNodesClusterState(currentState, remainingNodesBuilder));
                        return remainingNodesClusterState.get();
                    }
                };

        final DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        final int nodes = randomIntBetween(2, 16);
        final List<ZenDiscovery.NodeRemovalClusterStateTaskExecutor.Task> tasks = new ArrayList<>();
        // to ensure there is at least one removal
        boolean first = true;
        for (int i = 0; i < nodes; i++) {
            final DiscoveryNode node = node(i);
            builder.put(node);
            if (first || randomBoolean()) {
                tasks.add(new ZenDiscovery.NodeRemovalClusterStateTaskExecutor.Task(node, randomBoolean() ? "left" : "failed"));
            }
            first = false;
        }
        final ClusterState clusterState = ClusterState.builder(new ClusterName("test")).nodes(builder).build();

        final ClusterStateTaskExecutor.BatchResult<ZenDiscovery.NodeRemovalClusterStateTaskExecutor.Task> result =
                executor.execute(clusterState, tasks);

        assertEquals(capturedNodes.get(), remainingNodesClusterState.get().nodes());

        assertTrue(rejoined.get());
        assertThat(result.resultingState, equalTo(rejoinedClusterState.get()));

        for (final ZenDiscovery.NodeRemovalClusterStateTaskExecutor.Task task : tasks) {
            assertNull(result.resultingState.nodes().get(task.node().getId()));
        }
    }

    public void testRerouteAfterRemovingNodes() throws Exception {
        final AtomicReference<Iterable<DiscoveryNode>> capturedNodes = new AtomicReference<>();
        final ElectMasterService electMasterService = new ElectMasterService(Settings.EMPTY, Version.CURRENT) {
            @Override
            public boolean hasEnoughMasterNodes(Iterable<DiscoveryNode> nodes) {
                capturedNodes.set(nodes);
                return true;
            }
        };

        final AtomicReference<ClusterState> rerouteClusterState = new AtomicReference<>();
        final AllocationService allocationService = new AllocationService(Settings.EMPTY, null, null, null) {
            @Override
            public RoutingAllocation.Result reroute(ClusterState clusterState, String reason) {
                rerouteClusterState.set(clusterState);
                return new RoutingAllocation.Result(randomBoolean(), null);
            }
        };

        final ZenDiscovery.NodeRemovalClusterStateTaskExecutor.Rejoin rejoin = new ZenDiscovery.NodeRemovalClusterStateTaskExecutor.Rejoin() {
            @Override
            public ClusterState apply(ClusterState clusterState, String reason) {
                fail("rejoin should not be invoked");
                return clusterState;
            }
        };

        final AtomicReference<ClusterState> remainingNodesClusterState = new AtomicReference<>();
        final ZenDiscovery.NodeRemovalClusterStateTaskExecutor executor =
                new ZenDiscovery.NodeRemovalClusterStateTaskExecutor(allocationService, electMasterService, rejoin, logger) {
                    @Override
                    ClusterState remainingNodesClusterState(ClusterState currentState, DiscoveryNodes.Builder remainingNodesBuilder) {
                        remainingNodesClusterState.set(super.remainingNodesClusterState(currentState, remainingNodesBuilder));
                        return remainingNodesClusterState.get();
                    }
                };

        final DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        final int nodes = randomIntBetween(2, 16);
        final List<ZenDiscovery.NodeRemovalClusterStateTaskExecutor.Task> tasks = new ArrayList<>();
        // to ensure that there is at least one removal
        boolean first = true;
        for (int i = 0; i < nodes; i++) {
            final DiscoveryNode node = node(i);
            builder.put(node);
            if (first || randomBoolean()) {
                tasks.add(new ZenDiscovery.NodeRemovalClusterStateTaskExecutor.Task(node, randomBoolean() ? "left" : "failed"));
            }
            first = false;
        }
        final ClusterState clusterState = ClusterState.builder(new ClusterName("test")).nodes(builder).build();

        final ClusterStateTaskExecutor.BatchResult<ZenDiscovery.NodeRemovalClusterStateTaskExecutor.Task> result =
                executor.execute(clusterState, tasks);
        assertEquals(capturedNodes.get(), remainingNodesClusterState.get().nodes());
        assertEquals(rerouteClusterState.get(), remainingNodesClusterState.get());

        for (final ZenDiscovery.NodeRemovalClusterStateTaskExecutor.Task task : tasks) {
            assertNull(result.resultingState.nodes().get(task.node().getId()));
        }
    }

    private DiscoveryNode node(final int id) {
        return new DiscoveryNode(Integer.toString(id), LocalTransportAddress.buildUnique(), Version.CURRENT);
    }

}
