/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.persistent;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.Assignment;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.PersistentTask;
import org.elasticsearch.tasks.TaskId;

import java.util.Collection;
import java.util.Map;
import java.util.function.Predicate;

/**
 * An executor of tasks that can survive restart of requesting or executing node.
 * These tasks are using cluster state rather than only transport service to send requests and responses.
 */
public abstract class PersistentTasksExecutor<Params extends PersistentTaskParams> {

    private final String executor;
    private final String taskName;

    protected PersistentTasksExecutor(String taskName, String executor) {
        this.taskName = taskName;
        this.executor = executor;
    }

    public String getTaskName() {
        return taskName;
    }

    public static final Assignment NO_NODE_FOUND = new Assignment(null, "no appropriate nodes found for the assignment");

    /**
     * Returns the node id where the params has to be executed,
     * <p>
     * The default implementation returns the least loaded data node from amongst the collection of candidate nodes
     */
    public Assignment getAssignment(Params params, Collection<DiscoveryNode> candidateNodes, ClusterState clusterState) {
        DiscoveryNode discoveryNode = selectLeastLoadedNode(clusterState, candidateNodes, DiscoveryNode::canContainData);
        if (discoveryNode == null) {
            return NO_NODE_FOUND;
        } else {
            return new Assignment(discoveryNode.getId(), "");
        }
    }

    /**
     * Finds the least loaded node from amongs the candidate node collection
     * that satisfies the selector criteria
     */
    protected DiscoveryNode selectLeastLoadedNode(
        ClusterState clusterState,
        Collection<DiscoveryNode> candidateNodes,
        Predicate<DiscoveryNode> selector
    ) {
        long minLoad = Long.MAX_VALUE;
        DiscoveryNode minLoadedNode = null;
        PersistentTasksCustomMetadata persistentTasks = clusterState.getMetadata().custom(PersistentTasksCustomMetadata.TYPE);
        for (DiscoveryNode node : candidateNodes) {
            if (selector.test(node)) {
                if (persistentTasks == null) {
                    // We don't have any task running yet, pick the first available node
                    return node;
                }
                long numberOfTasks = persistentTasks.getNumberOfTasksOnNode(node.getId(), taskName);
                if (minLoad > numberOfTasks) {
                    minLoad = numberOfTasks;
                    minLoadedNode = node;
                }
            }
        }
        return minLoadedNode;
    }

    /**
     * Checks the current cluster state for compatibility with the params
     * <p>
     * Throws an exception if the supplied params cannot be executed on the cluster in the current state.
     */
    public void validate(Params params, ClusterState clusterState) {}

    /**
     * Creates a AllocatedPersistentTask for communicating with task manager
     */
    protected AllocatedPersistentTask createTask(
        long id,
        String type,
        String action,
        TaskId parentTaskId,
        PersistentTask<Params> taskInProgress,
        Map<String, String> headers
    ) {
        return new AllocatedPersistentTask(id, type, action, getDescription(taskInProgress), parentTaskId, headers);
    }

    /**
     * Returns task description that will be available via task manager
     */
    protected String getDescription(PersistentTask<Params> taskInProgress) {
        return "id=" + taskInProgress.getId();
    }

    /**
     * This operation will be executed on the executor node.
     * <p>
     * NOTE: The nodeOperation has to throw an exception, trigger task.markAsCompleted() or task.completeAndNotifyIfNeeded() methods to
     * indicate that the persistent task has finished.
     */
    protected abstract void nodeOperation(AllocatedPersistentTask task, Params params, @Nullable PersistentTaskState state);

    public String getExecutor() {
        return executor;
    }
}
