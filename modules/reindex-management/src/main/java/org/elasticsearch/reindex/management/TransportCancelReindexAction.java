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
        cancelTasksRequest.setParentTaskOnly(true);
        cancelTasksRequest.setWaitForCompletion(false);

        client.execute(TransportCancelTasksAction.TYPE, cancelTasksRequest, listener.delegateFailureAndWrap((l, cancelResponse) -> {
            // Maps validation and resource not found failures to reindex specific message
            if (taskNotFoundFailures(cancelResponse)) {
                logger.debug("cancel-tasks rejected task [{}]; reporting as reindex not found", taskId);
                l.onFailure(notFoundException(taskId));
                return;
            }
            // Surfaces real failures (e.g. 409 CONFLICT from a relocation handoff, transport-level node failures).
            cancelResponse.rethrowFailures("cancel_reindex");

            // No failure was reported but no task was matched either: treat as not found.
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
     * Returns {@code true} when the cancel-tasks response indicates the target reindex task could not be located or did not qualify
     * (non-reindex action, sub-task, non-cancellable). All such failures are thrown by
     * {@link TransportCancelTasksAction#processTasks} and surface as node failures whose cause unwraps to either
     * {@link ResourceNotFoundException} or {@link IllegalArgumentException}. We additionally require that no task failures are present,
     * because a task failure (e.g. the relocation-aware {@code 409 CONFLICT} from {@code ensureCancellable}) signals a real cancel failure
     * that must be surfaced to the caller rather than masked as not-found.
     */
    private static boolean taskNotFoundFailures(final ListTasksResponse response) {
        if (response.getTaskFailures().isEmpty() == false) {
            return false;
        }
        if (response.getNodeFailures().isEmpty()) {
            return false;
        }
        for (var failure : response.getNodeFailures()) {
            if (ExceptionsHelper.unwrap(failure, ResourceNotFoundException.class, IllegalArgumentException.class) == null) {
                return false;
            }
        }
        return true;
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
