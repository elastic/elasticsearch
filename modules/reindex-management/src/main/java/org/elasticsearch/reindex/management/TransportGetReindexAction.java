/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex.management;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.TaskRelocatedException;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskResult;
import org.elasticsearch.transport.TransportService;

/**
 * Transport action for getting a reindex task. Validates that the requested task is a reindex parent task,
 * then follows any relocation chain so the caller transparently sees the current state of a relocated reindex.
 */
public class TransportGetReindexAction extends HandledTransportAction<GetReindexRequest, GetReindexResponse> {
    public static final ActionType<GetReindexResponse> TYPE = new ActionType<>("cluster:monitor/reindex/get");

    private static final Logger logger = LogManager.getLogger(TransportGetReindexAction.class);

    private final Client client;

    @Inject
    public TransportGetReindexAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(TYPE.name(), transportService, actionFilters, GetReindexRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.client = client;
    }

    @Override
    protected void doExecute(Task thisTask, GetReindexRequest originalRequest, ActionListener<GetReindexResponse> listener) {
        // 1. Get the task without completion-waiting to check if the task exists and is a reindex task,
        // so later we don't wait if the request has `wait_for_completion=true` for a non-reindex task
        // 2. If the task was relocated, follow the relocation chain to get the latest task status
        // 3. Lastly, if the original request has wait_for_completion=true, issue another request to wait for reindex completion
        // Note that the underlying transport get task action is multi-project aware, so it will only return project specific tasks
        final TaskId taskId = originalRequest.getTaskId();
        final GetTaskRequest getTaskRequest = new GetTaskRequest().setTaskId(taskId).setWaitForCompletion(false);

        getTaskWithNotFoundHandling(getTaskRequest, taskId, listener.delegateFailureAndWrap((l, taskResult) -> {
            // Found a matching task by id, but it's not a reindex task, treat it as not found to prevent leaking other tasks
            if (ReindexAction.NAME.equals(taskResult.getTask().action()) == false) {
                logger.debug("task [{}] requested as reindex but is [{}], returning not found", taskId, taskResult.getTask().action());
                l.onFailure(notFoundException(taskId));
                return;
            }
            // Found a matching reindex task by id, but it's a reindex subtask, treat it as not found to hide slicing implementation
            // details
            if (taskResult.getTask().parentTaskId().isSet()) {
                logger.debug("reindex subtask [{}] requested directly, returning not found", taskId);
                l.onFailure(notFoundException(taskId));
                return;
            }

            final RelocatableReindexResult result = new RelocatableReindexResult(taskResult, null);
            followRelocationOrRespond(result, originalRequest, l);
        }));
    }

    /**
     * Follow the relocation chain if the current task was relocated.
     * The original task is preserved so the response can report the user-facing identity and start time.
     * Relocated tasks found in .tasks system index are trusted as reindex tasks without re-validation.
     */
    private void followRelocationOrRespond(
        final RelocatableReindexResult result,
        final GetReindexRequest originalRequest,
        final ActionListener<GetReindexResponse> listener
    ) {
        final TaskId relocatedTaskId = TaskRelocatedException.relocatedTaskIdFromErrorMap(result.errorAsMap()).orElse(null);
        if (relocatedTaskId == null) {
            buildResponse(result, originalRequest, listener);
        } else {
            logger.debug("task [{}] relocated to [{}], following", result.latestTaskId(), relocatedTaskId);
            final GetTaskRequest req = new GetTaskRequest().setTaskId(relocatedTaskId).setWaitForCompletion(false);
            getTaskWithNotFoundHandling(req, originalRequest.getTaskId(), listener.delegateFailureAndWrap((l, relocatedTask) -> {
                followRelocationOrRespond(result.withLatestRelocatedTask(relocatedTask), originalRequest, l);
            }));
        }
    }

    /**
     * Builds the final response. If {@code wait_for_completion} is requested and the active task is still
     * running, re-fetches with {@code waitForCompletion=true} before responding.
     */
    private void buildResponse(
        final RelocatableReindexResult result,
        final GetReindexRequest originalRequest,
        final ActionListener<GetReindexResponse> listener
    ) {
        if (result.isCompleted() || originalRequest.getWaitForCompletion() == false) {
            listener.onResponse(new GetReindexResponse(result));
            return;
        }

        // If waiting for an uncompleted task, we reissue the get request to wait for the reindex task to complete
        final GetTaskRequest finalWaitGetRequest = new GetTaskRequest().setTaskId(result.latestTaskId())
            .setWaitForCompletion(true)
            .setTimeout(originalRequest.getTimeout());

        getTaskWithNotFoundHandling(finalWaitGetRequest, originalRequest.getTaskId(), listener.delegateFailureAndWrap((l, finalResult) -> {
            // call back into followRelocationOrRespond to handle possible relocation which happened while we're waiting
            followRelocationOrRespond(result.withUpdatedResult(finalResult), originalRequest, l);
        }));
    }

    /** Get a task, mapping {@link ResourceNotFoundException} into a reindex-specific error to avoid leaking internal task details. */
    private void getTaskWithNotFoundHandling(
        final GetTaskRequest request,
        final TaskId originalTaskId,
        final ActionListener<TaskResult> listener
    ) {
        // Look for the reindex task on the node inferred from the task id for running reindex task,
        // or from the ".tasks" system index for completed tasks
        client.admin().cluster().getTask(request, new ActionListener<>() {
            @Override
            public void onResponse(final GetTaskResponse response) {
                listener.onResponse(response.getTask());
            }

            @Override
            public void onFailure(final Exception e) {
                if (e instanceof ResourceNotFoundException) {
                    final TaskId currentTaskId = request.getTaskId();
                    if (currentTaskId.equals(originalTaskId)) {
                        logger.debug("task [{}] not found, returning as reindex not found", originalTaskId);
                    } else {
                        logger.warn(
                            "task [{}] was eventually relocated to [{}] but can't be found, returning as reindex not found",
                            originalTaskId,
                            currentTaskId
                        );
                    }
                    // Wraps the task not found exception to hide task details
                    listener.onFailure(notFoundException(originalTaskId));
                } else {
                    listener.onFailure(e);
                }
            }
        });
    }

    // visible for testing
    static ResourceNotFoundException notFoundException(TaskId taskId) {
        return new ResourceNotFoundException("Reindex operation [{}] not found", taskId);
    }
}
