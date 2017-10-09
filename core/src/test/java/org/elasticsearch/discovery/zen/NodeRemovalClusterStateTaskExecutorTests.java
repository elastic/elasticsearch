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
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class NodeRemovalClusterStateTaskExecutorTests extends ESTestCase {

    public void testRemovingNonExistentNodes() throws Exception {
        final ZenDiscovery.NodeRemovalClusterStateTaskExecutor executor =
                new ZenDiscovery.NodeRemovalClusterStateTaskExecutor(null, null, null, logger);
        final DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        final int nodes = randomIntBetween(2, 16);
        for (int i = 0; i < nodes; i++) {
            builder.add(node(i));
        }
        final ClusterState clusterState = ClusterState.builder(new ClusterName("test")).nodes(builder).build();

        final DiscoveryNodes.Builder removeBuilder = DiscoveryNodes.builder();
        for (int i = nodes; i < nodes + randomIntBetween(1, 16); i++) {
            removeBuilder.add(node(i));
        }
        final List<ZenDiscovery.NodeRemovalClusterStateTaskExecutor.Task> tasks =
                StreamSupport
                        .stream(removeBuilder.build().spliterator(), false)
                        .map(node -> new ZenDiscovery.NodeRemovalClusterStateTaskExecutor.Task(node, randomBoolean() ? "left" : "failed"))
                        .collect(Collectors.toList());

        final ClusterStateTaskExecutor.ClusterTasksResult<ZenDiscovery.NodeRemovalClusterStateTaskExecutor.Task> result
                = executor.execute(clusterState, tasks);
        assertThat(result.resultingState, equalTo(clusterState));
    }

    public void testNotEnoughMasterNodesAfterRemove() throws Exception {
        final ElectMasterService electMasterService = mock(ElectMasterService.class);
        when(electMasterService.hasEnoughMasterNodes(any(Iterable.class))).thenReturn(false);

        final AllocationService allocationService = mock(AllocationService.class);

        final AtomicBoolean rejoinCalled = new AtomicBoolean();
        final Consumer<String> submitRejoin = source -> rejoinCalled.set(true);

        final AtomicReference<ClusterState> remainingNodesClusterState = new AtomicReference<>();
        final ZenDiscovery.NodeRemovalClusterStateTaskExecutor executor =
                new ZenDiscovery.NodeRemovalClusterStateTaskExecutor(allocationService, electMasterService, submitRejoin, logger) {
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
            builder.add(node);
            if (first || randomBoolean()) {
                tasks.add(new ZenDiscovery.NodeRemovalClusterStateTaskExecutor.Task(node, randomBoolean() ? "left" : "failed"));
            }
            first = false;
        }
        final ClusterState clusterState = ClusterState.builder(new ClusterName("test")).nodes(builder).build();

        final ClusterStateTaskExecutor.ClusterTasksResult<ZenDiscovery.NodeRemovalClusterStateTaskExecutor.Task> result =
                executor.execute(clusterState, tasks);
        verify(electMasterService).hasEnoughMasterNodes(eq(remainingNodesClusterState.get().nodes()));
        verify(electMasterService).countMasterNodes(eq(remainingNodesClusterState.get().nodes()));
        verify(electMasterService).minimumMasterNodes();
        verifyNoMoreInteractions(electMasterService);

        // ensure that we did not reroute
        verifyNoMoreInteractions(allocationService);
        assertTrue(rejoinCalled.get());
        assertThat(result.resultingState, equalTo(clusterState));

        for (final ZenDiscovery.NodeRemovalClusterStateTaskExecutor.Task task : tasks) {
            assertNotNull(result.resultingState.nodes().get(task.node().getId()));
        }
    }

    public void testRerouteAfterRemovingNodes() throws Exception {
        final ElectMasterService electMasterService = mock(ElectMasterService.class);
        when(electMasterService.hasEnoughMasterNodes(any(Iterable.class))).thenReturn(true);

        final AllocationService allocationService = mock(AllocationService.class);
        when(allocationService.deassociateDeadNodes(any(ClusterState.class), eq(true), any(String.class)))
            .thenAnswer(im -> im.getArguments()[0]);

        final Consumer<String> submitRejoin = source -> fail("rejoin should not be invoked");

        final AtomicReference<ClusterState> remainingNodesClusterState = new AtomicReference<>();
        final ZenDiscovery.NodeRemovalClusterStateTaskExecutor executor =
                new ZenDiscovery.NodeRemovalClusterStateTaskExecutor(allocationService, electMasterService, submitRejoin, logger) {
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
            builder.add(node);
            if (first || randomBoolean()) {
                tasks.add(new ZenDiscovery.NodeRemovalClusterStateTaskExecutor.Task(node, randomBoolean() ? "left" : "failed"));
            }
            first = false;
        }
        final ClusterState clusterState = ClusterState.builder(new ClusterName("test")).nodes(builder).build();

        final ClusterStateTaskExecutor.ClusterTasksResult<ZenDiscovery.NodeRemovalClusterStateTaskExecutor.Task> result =
                executor.execute(clusterState, tasks);
        verify(electMasterService).hasEnoughMasterNodes(eq(remainingNodesClusterState.get().nodes()));
        verifyNoMoreInteractions(electMasterService);

        verify(allocationService).deassociateDeadNodes(eq(remainingNodesClusterState.get()), eq(true), any(String.class));

        for (final ZenDiscovery.NodeRemovalClusterStateTaskExecutor.Task task : tasks) {
            assertNull(result.resultingState.nodes().get(task.node().getId()));
        }
    }

    private DiscoveryNode node(final int id) {
        return new DiscoveryNode(Integer.toString(id), buildNewFakeTransportAddress(), Version.CURRENT);
    }

}
