/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.node.tasks.cancel;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;

/**
 * Transport action that can be used to cancel currently running cancellable tasks.
 * <p>
 * For a task to be cancellable it has to return an instance of
 * {@link CancellableTask} from {@link TransportRequest#createTask}
 */
public class TransportCancelTasksAction extends TransportTasksAction<CancellableTask, CancelTasksRequest, ListTasksResponse, TaskInfo> {

    public static final String NAME = "cluster:admin/tasks/cancel";

    public static final ActionType<ListTasksResponse> TYPE = new ActionType<>(NAME);

    @Inject
    public TransportCancelTasksAction(ClusterService clusterService, TransportService transportService, ActionFilters actionFilters) {
        super(
            NAME,
            clusterService,
            transportService,
            actionFilters,
            CancelTasksRequest::new,
            TaskInfo::from,
            // Cancellation is usually lightweight, and runs on the transport thread if the task didn't even start yet, but some
            // implementations of CancellableTask#onCancelled() are nontrivial so we use GENERIC here. TODO could it be SAME?
            transportService.getThreadPool().executor(ThreadPool.Names.GENERIC)
        );
    }

    @Override
    protected ListTasksResponse newResponse(
        CancelTasksRequest request,
        List<TaskInfo> tasks,
        List<TaskOperationFailure> taskOperationFailures,
        List<FailedNodeException> failedNodeExceptions
    ) {
        if (request.getTargetTaskId().isSet() && tasks.isEmpty() && taskOperationFailures.isEmpty() && failedNodeExceptions.isEmpty()) {
            // For BwC: Now that we fan out to every node (to find potentially relocated targets by their original task id), the node
            // identified by the target task id no longer throws ResourceNotFoundException from its per-node processTasks.
            // Re-surface the same ResourceNotFoundException wrapped in a FailedNodeException if no task is found by the taget task Id.
            final TaskId target = request.getTargetTaskId();
            final FailedNodeException notFound = new FailedNodeException(
                target.getNodeId(),
                "Failed node [" + target.getNodeId() + "]",
                new ResourceNotFoundException("task [{}] is not found", target)
            );
            return new ListTasksResponse(tasks, taskOperationFailures, List.of(notFound));
        }
        return new ListTasksResponse(tasks, taskOperationFailures, failedNodeExceptions);
    }

    /**
     * Default fan out to every node in the cluster to find tasks potentially relocated to other nodes
     */
    @Override
    protected String[] resolveNodes(CancelTasksRequest request, DiscoveryNodes discoveryNodes) {
        return discoveryNodes.resolveNodes(request.getNodes());
    }

    protected List<CancellableTask> processTasks(CancelTasksRequest request) {
        if (request.getTargetTaskId().isSet()) {
            return findTargetTask(request);
        } else {
            final var tasks = new ArrayList<CancellableTask>();
            for (CancellableTask task : taskManager.getCancellableTasks().values()) {
                if (request.match(task) && match(task)) {
                    tasks.add(task);
                }
            }
            return tasks;
        }
    }

    /**
     * Locates the target task for a specific target task by task id, or by original task id for relocated tasks.
     */
    private List<CancellableTask> findTargetTask(CancelTasksRequest request) {
        final TaskId target = request.getTargetTaskId();
        // match by task id
        if (clusterService.localNode().getId().equals(target.getNodeId())) {
            final CancellableTask task = taskManager.getCancellableTask(target.getId());
            if (task != null) {
                if (request.match(task)) {
                    return List.of(task);
                } else {
                    throw new IllegalArgumentException("task [" + target + "] doesn't support this operation");
                }
            }
            // The task exists, but doesn't support cancellation
            if (taskManager.getTask(target.getId()) != null) {
                throw new IllegalArgumentException("task [" + target + "] doesn't support cancellation");
            }
        }
        // match by original task id for relocated tasks
        for (CancellableTask task : taskManager.getCancellableTasks().values()) {
            if (task.getOriginalTaskId().filter(target::equals).isPresent() && request.match(task)) {
                return List.of(task);
            }
        }
        return List.of();
    }

    @Override
    protected void taskOperation(
        CancellableTask actionTask,
        CancelTasksRequest request,
        CancellableTask cancellableTask,
        ActionListener<TaskInfo> listener
    ) {
        cancellableTask.ensureCancellable();
        String nodeId = clusterService.localNode().getId();
        taskManager.cancelTaskAndDescendants(
            cancellableTask,
            request.getReason(),
            request.waitForCompletion(),
            listener.map(r -> cancellableTask.taskInfo(nodeId, false))
        );
    }
}
