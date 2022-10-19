/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;

public class NodeRemovalClusterStateTaskExecutor implements ClusterStateTaskExecutor<NodeRemovalClusterStateTaskExecutor.Task> {

    private static final Logger logger = LogManager.getLogger(NodeRemovalClusterStateTaskExecutor.class);

    private final AllocationService allocationService;

    public record Task(DiscoveryNode node, String reason, Runnable onClusterStateProcessed) implements ClusterStateTaskListener {

        @Override
        public void onFailure(final Exception e) {
            logger.log(MasterService.isPublishFailureException(e) ? Level.DEBUG : Level.ERROR, "unexpected failure during [node-left]", e);
        }

        @Override
        public String toString() {
            final StringBuilder stringBuilder = new StringBuilder();
            node.appendDescriptionWithoutAttributes(stringBuilder);
            stringBuilder.append(" reason: ").append(reason);
            return stringBuilder.toString();
        }
    }

    public NodeRemovalClusterStateTaskExecutor(AllocationService allocationService) {
        this.allocationService = allocationService;
    }

    @Override
    public ClusterState execute(BatchExecutionContext<Task> batchExecutionContext) throws Exception {
        final ClusterState initialState = batchExecutionContext.initialState();
        final DiscoveryNodes.Builder remainingNodesBuilder = DiscoveryNodes.builder(initialState.nodes());
        boolean removed = false;
        for (final var taskContext : batchExecutionContext.taskContexts()) {
            final var task = taskContext.getTask();
            if (initialState.nodes().nodeExists(task.node())) {
                remainingNodesBuilder.remove(task.node());
                removed = true;
            } else {
                logger.debug("node [{}] does not exist in cluster state, ignoring", task);
            }
            taskContext.success(task.onClusterStateProcessed::run);
        }

        if (removed == false) {
            // no nodes to remove, keep the current cluster state
            return initialState;
        }

        try (var ignored = batchExecutionContext.dropHeadersContext()) {
            // suppress deprecation warnings e.g. from reroute()

            final var remainingNodesClusterState = remainingNodesClusterState(initialState, remainingNodesBuilder);
            final var ptasksDisassociatedState = PersistentTasksCustomMetadata.disassociateDeadNodes(remainingNodesClusterState);
            return allocationService.disassociateDeadNodes(
                ptasksDisassociatedState,
                true,
                describeTasks(batchExecutionContext.taskContexts().stream().map(TaskContext::getTask).toList())
            );
        }
    }

    // visible for testing
    // hook is used in testing to ensure that correct cluster state is used to test whether a
    // rejoin or reroute is needed
    protected ClusterState remainingNodesClusterState(final ClusterState currentState, DiscoveryNodes.Builder remainingNodesBuilder) {
        return ClusterState.builder(currentState).nodes(remainingNodesBuilder).build();
    }

}
