/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex.management;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.NoSuchNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksProjectAction;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskResult;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;

/** Transport action that cancels an in-flight reindex task and its descendants. */
public class TransportCancelReindexAction extends TransportTasksProjectAction<
    CancellableTask,
    CancelReindexRequest,
    CancelReindexResponse,
    CancelReindexTaskResponse> {

    public static final ActionType<CancelReindexResponse> TYPE = new ActionType<>("cluster:admin/reindex/cancel");

    @Inject
    public TransportCancelReindexAction(
        final ClusterService clusterService,
        final TransportService transportService,
        final ActionFilters actionFilters,
        final ProjectResolver projectResolver
    ) {
        super(
            TYPE.name(),
            clusterService,
            transportService,
            actionFilters,
            CancelReindexRequest::new,
            CancelReindexTaskResponse::new,
            transportService.getThreadPool().executor(ThreadPool.Names.GENERIC),
            projectResolver
        );
    }

    @Override
    protected List<CancellableTask> processTasks(final CancelReindexRequest request) {
        final CancellableTask requestedTask = taskManager.getCancellableTask(request.getTargetTaskId().getId());
        if (requestedTask != null && super.match(requestedTask) && request.match(requestedTask)) {
            return List.of(requestedTask);
        }
        return List.of();
    }

    @Override
    protected void taskOperation(
        final CancellableTask actionTask,
        final CancelReindexRequest request,
        final CancellableTask task,
        final ActionListener<CancelReindexTaskResponse> listener
    ) {
        assert task instanceof BulkByScrollTask : "Task should be a BulkByScrollTask";

        taskManager.cancelTaskAndDescendants(
            task,
            CancelTasksRequest.DEFAULT_REASON,
            request.waitForCompletion(),
            ActionListener.wrap(ignored -> {
                final TaskResult completedTaskResult = request.waitForCompletion()
                    ? new TaskResult(true, task.taskInfo(clusterService.localNode().getId(), true))
                    : null;
                listener.onResponse(new CancelReindexTaskResponse(completedTaskResult));
            }, listener::onFailure)
        );
    }

    @Override
    protected CancelReindexResponse newResponse(
        final CancelReindexRequest request,
        final List<CancelReindexTaskResponse> tasks,
        final List<TaskOperationFailure> taskFailures,
        final List<FailedNodeException> nodeExceptions
    ) {
        assert tasks.size() + taskFailures.size() + nodeExceptions.size() <= 1 : "currently only supports cancelling one task max";
        // check whether node in requested TaskId doesn't exist and throw 404
        for (final FailedNodeException e : nodeExceptions) {
            if (ExceptionsHelper.unwrap(e, NoSuchNodeException.class) != null) {
                throw reindexWithTaskIdNotFoundException(request.getTargetTaskId());
            }
        }

        final GetReindexResponse completedReindexResponse = tasks.isEmpty()
            ? null
            : tasks.getFirst().getCompletedTaskResult().map(GetReindexResponse::new).orElse(null);
        final var response = new CancelReindexResponse(taskFailures, nodeExceptions, completedReindexResponse);
        response.rethrowFailures("cancel_reindex"); // if we haven't handled any exception already, throw here
        if (tasks.isEmpty()) {
            throw reindexWithTaskIdNotFoundException(request.getTargetTaskId());
        }
        return response;
    }

    private static ResourceNotFoundException reindexWithTaskIdNotFoundException(final TaskId requestedTaskId) {
        return new ResourceNotFoundException("reindex task [{}] either not found or completed", requestedTaskId);
    }
}
