/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex.management;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.TransportCancelTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.tasks.TaskResult;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Objects;

/** Transport action that cancels an in-flight reindex task and its descendants. */
public class TransportCancelReindexAction extends HandledTransportAction<CancelReindexRequest, CancelReindexResponse> {

    public static final ActionType<CancelReindexResponse> TYPE = new ActionType<>("cluster:admin/reindex/cancel");

    private static final Logger logger = LogManager.getLogger(TransportCancelReindexAction.class);

    private final Client client;

    @Inject
    public TransportCancelReindexAction(final TransportService transportService, final ActionFilters actionFilters, final Client client) {
        super(
            TYPE.name(),
            transportService,
            actionFilters,
            CancelReindexRequest::new,
            transportService.getThreadPool().executor(ThreadPool.Names.GENERIC)
        );
        this.client = Objects.requireNonNull(client);
    }

    @Override
    protected void doExecute(
        final Task thisTask,
        final CancelReindexRequest request,
        final ActionListener<CancelReindexResponse> listener
    ) {
        final TaskId taskId = request.getTaskId();

        final CancelTasksRequest cancelTasksRequest = new CancelTasksRequest();
        cancelTasksRequest.setTargetTaskId(taskId);
        cancelTasksRequest.setActions(ReindexAction.NAME);
        cancelTasksRequest.setExcludeChildTasks(true);
        cancelTasksRequest.setWaitForCompletion(false);

        client.execute(TransportCancelTasksAction.TYPE, cancelTasksRequest, listener.delegateFailureAndWrap((l, cancelResponse) -> {
            // Maps validation and resource not found failures to reindex specific error
            if (shouldTreatAsNotFound(cancelResponse)) {
                logger.debug("cancel-tasks rejected task [{}]; reporting as reindex not found", taskId);
                l.onFailure(notFoundException(taskId));
                return;
            }
            // Surface other failures as-is
            final Exception cancelFailure = cancelFailureFrom(cancelResponse);
            if (cancelFailure != null) {
                l.onFailure(cancelFailure);
                return;
            }
            // No failure was reported but no task was matched either
            if (cancelResponse.getTasks().isEmpty()) {
                l.onFailure(notFoundException(taskId));
                return;
            }

            if (request.waitForCompletion()) {
                // Fetch the completed reindex result with relocation following so the response reflects the original task identity.
                final GetReindexRequest getRequest = new GetReindexRequest(taskId, true, null);
                client.execute(
                    TransportGetReindexAction.TYPE,
                    getRequest,
                    l.delegateFailureAndWrap(
                        // Force cancelled=true: the stored TaskInfo may not reflect the cancellation flag.
                        (l2, getResp) -> l2.onResponse(
                            new CancelReindexResponse(new GetReindexResponse(taskResultWithCancelledTrue(getResp.getTaskResult())))
                        )
                    )
                );
            } else {
                l.onResponse(new CancelReindexResponse((GetReindexResponse) null));
            }
        }));
    }

    /**
     * Returns the real failure to surface from a cancel-tasks response, with any additional real failures attached as suppressed
     * exceptions, or {@code null} when there are none. Validation-style node failures (handled separately by
     * {@link #shouldTreatAsNotFound}) are skipped.
     */
    private static Exception cancelFailureFrom(final ListTasksResponse response) {
        Exception head = null;
        for (var nodeFailure : response.getNodeFailures()) {
            if (isTaskValidationFailure(nodeFailure)) {
                continue;
            }
            head = ExceptionsHelper.useOrSuppress(head, nodeFailure);
        }
        for (var taskFailure : response.getTaskFailures()) {
            head = ExceptionsHelper.useOrSuppress(head, taskFailure.getCause());
        }
        return head;
    }

    /**
     * Returns {@code true} when the cancel-tasks response indicates the target reindex task could not be located or did not qualify
     * (non-reindex action, sub-task, non-cancellable). Such failures arrive as node failures whose cause is
     * {@link ResourceNotFoundException} or {@link IllegalArgumentException} thrown by {@link TransportCancelTasksAction}.
     * <p>
     * We also require that no task failures are present, because a task failure signals a real cancel failure that must be surfaced
     * to the caller.
     */
    private static boolean shouldTreatAsNotFound(final ListTasksResponse response) {
        if (response.getTaskFailures().isEmpty() == false) {
            return false;
        }
        if (response.getNodeFailures().isEmpty()) {
            return false;
        }
        for (var failure : response.getNodeFailures()) {
            if (isTaskValidationFailure(failure) == false) {
                return false;
            }
        }
        return true;
    }

    /**
     * True iff the given node failure unwraps to one of the validation-style exceptions
     * {@link TransportCancelTasksAction#processTasks} throws when the request targets a single task that doesn't qualify.
     */
    private static boolean isTaskValidationFailure(final ElasticsearchException failure) {
        if (ExceptionsHelper.unwrap(failure, ResourceNotFoundException.class) != null) {
            return true;
        }
        final IllegalArgumentException iae = (IllegalArgumentException) ExceptionsHelper.unwrap(failure, IllegalArgumentException.class);
        if (iae == null) {
            return false;
        }
        final String message = iae.getMessage();
        return message != null && (message.contains("doesn't support this operation") || message.contains("doesn't support cancellation"));
    }

    static ResourceNotFoundException notFoundException(final TaskId taskId) {
        return new ResourceNotFoundException("reindex task [{}] either not found or completed", taskId);
    }

    private static TaskResult taskResultWithCancelledTrue(final TaskResult r) {
        final TaskInfo taskInfo = r.getTask();
        final TaskInfo newTaskInfo = new TaskInfo(
            taskInfo.taskId(),
            taskInfo.type(),
            taskInfo.node(),
            taskInfo.action(),
            taskInfo.description(),
            taskInfo.status(),
            taskInfo.startTime(),
            taskInfo.runningTimeNanos(),
            taskInfo.cancellable(),
            true,
            taskInfo.parentTaskId(),
            taskInfo.headers()
        );
        return new TaskResult(r.isCompleted(), newTaskInfo, r.getError(), r.getResponse());
    }

}
