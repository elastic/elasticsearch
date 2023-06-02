/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.Level;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterStateTaskExecutorUtils;
import org.elasticsearch.common.Priority;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class NodeLeftExecutorTests extends ESTestCase {

    public void testRemovingNonExistentNodes() throws Exception {
        final NodeLeftExecutor executor = new NodeLeftExecutor(null);
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
        final List<NodeLeftExecutor.Task> tasks = removeBuilder.build()
            .stream()
            .map(node -> new NodeLeftExecutor.Task(node, randomBoolean() ? "left" : "failed", () -> {}))
            .toList();

        assertSame(clusterState, ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(clusterState, executor, tasks));
    }

    public void testRerouteAfterRemovingNodes() throws Exception {
        final AllocationService allocationService = mock(AllocationService.class);
        when(allocationService.disassociateDeadNodes(any(ClusterState.class), eq(true), any(String.class))).thenAnswer(
            im -> im.getArguments()[0]
        );

        final AtomicReference<ClusterState> remainingNodesClusterState = new AtomicReference<>();
        final NodeLeftExecutor executor = new NodeLeftExecutor(allocationService) {
            @Override
            protected ClusterState remainingNodesClusterState(
                ClusterState currentState,
                DiscoveryNodes.Builder remainingNodesBuilder,
                Map<String, TransportVersion> transportVersions
            ) {
                remainingNodesClusterState.set(super.remainingNodesClusterState(currentState, remainingNodesBuilder, transportVersions));
                return remainingNodesClusterState.get();
            }
        };

        final DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        final int nodes = randomIntBetween(2, 16);
        final List<NodeLeftExecutor.Task> tasks = new ArrayList<>();
        // to ensure that there is at least one removal
        boolean first = true;
        for (int i = 0; i < nodes; i++) {
            final DiscoveryNode node = node(i);
            builder.add(node);
            if (first || randomBoolean()) {
                tasks.add(new NodeLeftExecutor.Task(node, randomBoolean() ? "left" : "failed", () -> {}));
            }
            first = false;
        }
        final ClusterState clusterState = ClusterState.builder(new ClusterName("test")).nodes(builder).build();

        final var resultingState = ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(clusterState, executor, tasks);

        verify(allocationService).disassociateDeadNodes(eq(remainingNodesClusterState.get()), eq(true), any(String.class));

        for (final NodeLeftExecutor.Task task : tasks) {
            assertNull(resultingState.nodes().get(task.node().getId()));
        }
    }

    public void testPerNodeLogging() {
        final AllocationService allocationService = mock(AllocationService.class);
        when(allocationService.disassociateDeadNodes(any(ClusterState.class), eq(true), any(String.class))).thenAnswer(
            im -> im.getArguments()[0]
        );
        final var executor = new NodeLeftExecutor(allocationService);

        final DiscoveryNode masterNode = DiscoveryNodeUtils.create("master");
        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(
                DiscoveryNodes.builder()
                    .add(masterNode)
                    .localNodeId("master")
                    .masterNodeId("master")
                    .add(DiscoveryNodeUtils.create("other"))
            )
            .build();

        final MockLogAppender appender = new MockLogAppender();
        final ThreadPool threadPool = new TestThreadPool("test");
        try (
            var ignored = appender.capturing(NodeLeftExecutor.class);
            var clusterService = ClusterServiceUtils.createClusterService(clusterState, threadPool)
        ) {
            final var nodeToRemove = clusterState.nodes().get("other");
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "info message",
                    LOGGER_NAME,
                    Level.INFO,
                    "node-left: [" + nodeToRemove.descriptionWithoutAttributes() + "] with reason [test reason]"
                )
            );
            assertNull(
                PlainActionFuture.<Void, RuntimeException>get(
                    future -> clusterService.getMasterService()
                        .createTaskQueue("test", Priority.NORMAL, executor)
                        .submitTask("test", new NodeLeftExecutor.Task(nodeToRemove, "test reason", () -> future.onResponse(null)), null)
                )
            );
            appender.assertAllExpectationsMatched();
        } finally {
            TestThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    private static DiscoveryNode node(final int id) {
        return DiscoveryNodeUtils.create(Integer.toString(id));
    }

    // Hard-coding the class name here because it is also mentioned in the troubleshooting docs, so should not be renamed without care.
    private static final String LOGGER_NAME = "org.elasticsearch.cluster.coordination.NodeLeftExecutor";

}
