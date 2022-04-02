/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;

import java.util.List;

public class NodeRemovalClusterStateTaskExecutor implements ClusterStateTaskExecutor<NodeRemovalClusterStateTaskExecutor.Task> {

    private static final Logger logger = LogManager.getLogger(NodeRemovalClusterStateTaskExecutor.class);

    private final AllocationService allocationService;

    public record Task(DiscoveryNode node, String reason, Runnable onClusterStateProcessed) implements ClusterStateTaskListener {

        @Override
        public void onFailure(final Exception e) {
            logger.error("unexpected failure during [node-left]", e);
        }

        @Override
        public void onNoLongerMaster() {
            logger.debug("no longer master while processing node removal [node-left]");
        }

        @Override
        public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
            assert false : "not called";
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
    public ClusterState execute(ClusterState currentState, List<TaskContext<Task>> taskContexts) throws Exception {
        final DiscoveryNodes.Builder remainingNodesBuilder = DiscoveryNodes.builder(currentState.nodes());
        boolean removed = false;
        for (final var taskContext : taskContexts) {
            final var task = taskContext.getTask();
            if (currentState.nodes().nodeExists(task.node())) {
                remainingNodesBuilder.remove(task.node());
                removed = true;
            } else {
                logger.debug("node [{}] does not exist in cluster state, ignoring", task);
            }
            taskContext.success(new ActionListener<>() {
                @Override
                public void onResponse(ClusterState clusterState) {
                    task.onClusterStateProcessed.run();
                }

                @Override
                public void onFailure(Exception e) {
                    task.onFailure(e);
                }
            });
        }

        final ClusterState finalState;

        if (removed) {
            final ClusterState remainingNodesClusterState = remainingNodesClusterState(currentState, remainingNodesBuilder);
            final ClusterState ptasksDisassociatedState = PersistentTasksCustomMetadata.disassociateDeadNodes(remainingNodesClusterState);
            finalState = allocationService.disassociateDeadNodes(
                ptasksDisassociatedState,
                true,
                describeTasks(taskContexts.stream().map(TaskContext::getTask).toList())
            );
        } else {
            // no nodes to remove, keep the current cluster state
            finalState = currentState;
        }

        return finalState;
    }

    // visible for testing
    // hook is used in testing to ensure that correct cluster state is used to test whether a
    // rejoin or reroute is needed
    protected ClusterState remainingNodesClusterState(final ClusterState currentState, DiscoveryNodes.Builder remainingNodesBuilder) {
        return ClusterState.builder(currentState).nodes(remainingNodesBuilder).build();
    }

}
