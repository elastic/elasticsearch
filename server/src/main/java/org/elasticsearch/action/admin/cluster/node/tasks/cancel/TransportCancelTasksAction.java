/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.tasks.cancel;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.CancellableTask;
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
public class TransportCancelTasksAction extends TransportTasksAction<CancellableTask, CancelTasksRequest, CancelTasksResponse, TaskInfo> {

    @Inject
    public TransportCancelTasksAction(ClusterService clusterService, TransportService transportService, ActionFilters actionFilters) {
        super(
            CancelTasksAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            CancelTasksRequest::new,
            TaskInfo::from,
            // Cancellation is usually lightweight, and runs on the transport thread if the task didn't even start yet, but some
            // implementations of CancellableTask#onCancelled() are nontrivial so we use GENERIC here. TODO could it be SAME?
            ThreadPool.Names.GENERIC
        );
    }

    @Override
    protected CancelTasksResponse newResponse(
        CancelTasksRequest request,
        List<TaskInfo> tasks,
        List<TaskOperationFailure> taskOperationFailures,
        List<FailedNodeException> failedNodeExceptions
    ) {
        return new CancelTasksResponse(tasks, taskOperationFailures, failedNodeExceptions);
    }

    protected List<CancellableTask> processTasks(CancelTasksRequest request) {
        if (request.getTargetTaskId().isSet()) {
            // we are only checking one task, we can optimize it
            CancellableTask task = taskManager.getCancellableTask(request.getTargetTaskId().getId());
            if (task != null) {
                if (request.match(task)) {
                    return List.of(task);
                } else {
                    throw new IllegalArgumentException("task [" + request.getTargetTaskId() + "] doesn't support this operation");
                }
            } else {
                if (taskManager.getTask(request.getTargetTaskId().getId()) != null) {
                    // The task exists, but doesn't support cancellation
                    throw new IllegalArgumentException("task [" + request.getTargetTaskId() + "] doesn't support cancellation");
                } else {
                    throw new ResourceNotFoundException("task [{}] is not found", request.getTargetTaskId());
                }
            }
        } else {
            final var tasks = new ArrayList<CancellableTask>();
            for (CancellableTask task : taskManager.getCancellableTasks().values()) {
                if (request.match(task)) {
                    tasks.add(task);
                }
            }
            return tasks;
        }
    }

    @Override
    protected void taskOperation(
        CancellableTask actionTask,
        CancelTasksRequest request,
        CancellableTask cancellableTask,
        ActionListener<TaskInfo> listener
    ) {
        String nodeId = clusterService.localNode().getId();
        taskManager.cancelTaskAndDescendants(
            cancellableTask,
            request.getReason(),
            request.waitForCompletion(),
            listener.map(r -> cancellableTask.taskInfo(nodeId, false))
        );
    }
}
