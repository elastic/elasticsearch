/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination;

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
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class NodeRemovalClusterStateTaskExecutorTests extends ESTestCase {

    public void testRemovingNonExistentNodes() throws Exception {
        final NodeRemovalClusterStateTaskExecutor executor =
                new NodeRemovalClusterStateTaskExecutor(null, logger);
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
        final List<NodeRemovalClusterStateTaskExecutor.Task> tasks =
                StreamSupport
                        .stream(removeBuilder.build().spliterator(), false)
                        .map(node -> new NodeRemovalClusterStateTaskExecutor.Task(node, randomBoolean() ? "left" : "failed"))
                        .collect(Collectors.toList());

        final ClusterStateTaskExecutor.ClusterTasksResult<NodeRemovalClusterStateTaskExecutor.Task> result
                = executor.execute(clusterState, tasks);
        assertThat(result.resultingState, equalTo(clusterState));
    }

    public void testRerouteAfterRemovingNodes() throws Exception {
        final AllocationService allocationService = mock(AllocationService.class);
        when(allocationService.disassociateDeadNodes(any(ClusterState.class), eq(true), any(String.class)))
            .thenAnswer(im -> im.getArguments()[0]);

        final AtomicReference<ClusterState> remainingNodesClusterState = new AtomicReference<>();
        final NodeRemovalClusterStateTaskExecutor executor =
                new NodeRemovalClusterStateTaskExecutor(allocationService, logger) {
                    @Override
                    protected ClusterState remainingNodesClusterState(ClusterState currentState,
                                                                      DiscoveryNodes.Builder remainingNodesBuilder) {
                        remainingNodesClusterState.set(super.remainingNodesClusterState(currentState, remainingNodesBuilder));
                        return remainingNodesClusterState.get();
                    }
                };

        final DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        final int nodes = randomIntBetween(2, 16);
        final List<NodeRemovalClusterStateTaskExecutor.Task> tasks = new ArrayList<>();
        // to ensure that there is at least one removal
        boolean first = true;
        for (int i = 0; i < nodes; i++) {
            final DiscoveryNode node = node(i);
            builder.add(node);
            if (first || randomBoolean()) {
                tasks.add(new NodeRemovalClusterStateTaskExecutor.Task(node, randomBoolean() ? "left" : "failed"));
            }
            first = false;
        }
        final ClusterState clusterState = ClusterState.builder(new ClusterName("test")).nodes(builder).build();

        final ClusterStateTaskExecutor.ClusterTasksResult<NodeRemovalClusterStateTaskExecutor.Task> result =
                executor.execute(clusterState, tasks);

        verify(allocationService).disassociateDeadNodes(eq(remainingNodesClusterState.get()), eq(true), any(String.class));

        for (final NodeRemovalClusterStateTaskExecutor.Task task : tasks) {
            assertNull(result.resultingState.nodes().get(task.node().getId()));
        }
    }

    private DiscoveryNode node(final int id) {
        return new DiscoveryNode(Integer.toString(id), buildNewFakeTransportAddress(), Version.CURRENT);
    }

}
