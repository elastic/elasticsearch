/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.NoSuchNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/** Transport action that cancels an in-flight reindex task and its descendants. */
public class TransportCancelReindexAction extends TransportTasksAction<
    CancellableTask,
    CancelReindexRequest,
    CancelReindexResponse,
    CancelReindexTaskResponse> {

    public static final ActionType<CancelReindexResponse> TYPE = new ActionType<>("cluster:admin/reindex/cancel");
    private static final Logger LOG = LogManager.getLogger(TransportCancelReindexAction.class);

    private final ProjectResolver projectResolver;

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
            transportService.getThreadPool().executor(ThreadPool.Names.GENERIC)
        );
        this.projectResolver = Objects.requireNonNull(projectResolver, "projectResolver");
    }

    @Override
    protected List<CancellableTask> processTasks(final CancelReindexRequest request) {
        final TaskId taskId = request.getTargetTaskId();
        assert taskId.isSet() : "task id must be provided";

        final CancellableTask task = taskManager.getCancellableTask(taskId.getId());
        if (task == null) {
            LOG.debug("Task is missing. taskId='{}'", taskId);
            return List.of();
        }
        if (ReindexAction.NAME.equals(task.getAction()) == false) {
            LOG.debug("Task is not an reindex task. taskId='{}'", taskId);
            return List.of();
        }
        if (task.getParentTaskId().isSet()) {
            LOG.debug("Provided taskId is sub-task, can't cancel. taskId='{}'", taskId);
            return List.of();
        }

        final ProjectId requestProjectId = projectResolver.getProjectId();
        final String taskProjectId = task.getProjectId();
        if (taskProjectId == null) {
            if (ProjectId.DEFAULT.equals(requestProjectId) == false) {
                LOG.debug("requestProjectId={} != taskProjectId=null", requestProjectId);
                return List.of();
            }
        } else if (Objects.equals(requestProjectId.id(), taskProjectId) == false) {
            LOG.debug("requestProjectId={} != taskProjectId={}", requestProjectId, taskProjectId);
            return List.of();
        }

        return List.of(task);
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
            ActionListener.wrap(ignored -> listener.onResponse(new CancelReindexTaskResponse()), listener::onFailure)
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
        final Supplier<ResourceNotFoundException> notFoundSupplier = () -> new ResourceNotFoundException(
            "reindex task [{}] either not found or completed",
            request.getTargetTaskId()
        );
        for (FailedNodeException e : nodeExceptions) {
            if (ExceptionsHelper.unwrap(e, NoSuchNodeException.class) != null) {
                throw notFoundSupplier.get();
            }
        }

        final var response = new CancelReindexResponse(taskFailures, nodeExceptions);
        response.rethrowFailures("cancel_reindex"); // if we haven't handled any exception already, throw here
        if (tasks.isEmpty()) {
            throw notFoundSupplier.get();
        }
        return response;
    }
}
