/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.persistent;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportResponse.Empty;
import org.elasticsearch.xpack.persistent.PersistentTasksService.PersistentTaskOperationListener;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData.Assignment;

import java.util.function.Predicate;

/**
 * An executor of tasks that can survive restart of requesting or executing node.
 * These tasks are using cluster state rather than only transport service to send requests and responses.
 */
public abstract class PersistentTasksExecutor<Request extends PersistentTaskRequest> extends AbstractComponent {

    private final String executor;
    private final String taskName;
    private final PersistentTasksService persistentTasksService;

    protected PersistentTasksExecutor(Settings settings, String taskName, PersistentTasksService persistentTasksService,
                                      String executor) {
        super(settings);
        this.taskName = taskName;
        this.executor = executor;
        this.persistentTasksService = persistentTasksService;
    }

    public String getTaskName() {
        return taskName;
    }

    public static final Assignment NO_NODE_FOUND = new Assignment(null, "no appropriate nodes found for the assignment");

    /**
     * Returns the node id where the request has to be executed,
     * <p>
     * The default implementation returns the least loaded data node
     */
    public Assignment getAssignment(Request request, ClusterState clusterState) {
        DiscoveryNode discoveryNode = selectLeastLoadedNode(clusterState, DiscoveryNode::isDataNode);
        if (discoveryNode == null) {
            return NO_NODE_FOUND;
        } else {
            return new Assignment(discoveryNode.getId(), "");
        }
    }

    /**
     * Finds the least loaded node that satisfies the selector criteria
     */
    protected DiscoveryNode selectLeastLoadedNode(ClusterState clusterState, Predicate<DiscoveryNode> selector) {
        long minLoad = Long.MAX_VALUE;
        DiscoveryNode minLoadedNode = null;
        PersistentTasksCustomMetaData persistentTasks = clusterState.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
        for (DiscoveryNode node : clusterState.getNodes()) {
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
     * Checks the current cluster state for compatibility with the request
     * <p>
     * Throws an exception if the supplied request cannot be executed on the cluster in the current state.
     */
    public void validate(Request request, ClusterState clusterState) {

    }

    /**
     * Updates the persistent task status in the cluster state.
     * <p>
     * The status can be used to store the current progress of the task or provide an insight for the
     * task allocator about the state of the currently running tasks.
     */
    protected void updatePersistentTaskStatus(AllocatedPersistentTask task, Task.Status status, ActionListener<Empty> listener) {
        persistentTasksService.updateStatus(task.getPersistentTaskId(), status,
                new PersistentTaskOperationListener() {
                    @Override
                    public void onResponse(long taskId) {
                        listener.onResponse(Empty.INSTANCE);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        listener.onFailure(e);
                    }
                });
    }

    /**
     * This operation will be executed on the executor node.
     * <p>
     * If nodeOperation throws an exception or triggers listener.onFailure() method, the task will be restarted,
     * possibly on a different node. If listener.onResponse() is called, the task is considered to be successfully
     * completed and will be removed from the cluster state and not restarted.
     */
    protected abstract void nodeOperation(AllocatedPersistentTask task, Request request, ActionListener<Empty> listener);

    public String getExecutor() {
        return executor;
    }
}