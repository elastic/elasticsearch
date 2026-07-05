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
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskResult;
import org.elasticsearch.transport.TransportService;

import static org.elasticsearch.action.admin.cluster.node.tasks.get.TransportGetTaskAction.TASKS_ORIGIN;

/**
 * Transport action for getting a reindex task. Validates that the requested task is a reindex parent task,
 * then delegates to the relocation-aware Get Task API which transparently follows any relocation chain.
 */
public class TransportGetReindexAction extends HandledTransportAction<GetReindexRequest, GetReindexResponse> {
    public static final ActionType<GetReindexResponse> TYPE = new ActionType<>("cluster:monitor/reindex/get");

    private static final Logger logger = LogManager.getLogger(TransportGetReindexAction.class);

    private final Client client;

    @Inject
    public TransportGetReindexAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(TYPE.name(), transportService, actionFilters, GetReindexRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.client = new OriginSettingClient(client, TASKS_ORIGIN);
    }

    @Override
    protected void doExecute(Task thisTask, GetReindexRequest request, ActionListener<GetReindexResponse> listener) {
        final TaskId taskId = request.getTaskId();

        // Probe without waiting or following relocations, just to validate this is a reindex parent task
        final GetTaskRequest probeRequest = new GetTaskRequest().setTaskId(taskId).setWaitForCompletion(false).setFollowRelocations(false);

        getTask(probeRequest, taskId, listener.delegateFailureAndWrap((l, probeResult) -> {
            // Reject if the specified task is not a reindex parent task, to hide task or slicing implementation details
            if (ReindexAction.NAME.equals(probeResult.getTask().action()) == false) {
                logger.debug("task [{}] requested as reindex but is [{}], returning not found", taskId, probeResult.getTask().action());
                l.onFailure(notFoundException(taskId));
                return;
            }
            if (probeResult.getTask().parentTaskId().isSet()) {
                logger.debug("reindex subtask [{}] requested directly, returning not found", taskId);
                l.onFailure(notFoundException(taskId));
                return;
            }

            // Fetch the real result with relocation following and optional wait
            final GetTaskRequest fetchRequest = new GetTaskRequest().setTaskId(taskId)
                .setWaitForCompletion(request.getWaitForCompletion())
                .setTimeout(request.getTimeout());
            getTask(fetchRequest, taskId, l.delegateFailureAndWrap((l2, result) -> l2.onResponse(new GetReindexResponse(result))));
        }));
    }

    /** Fetches a task, replacing {@link ResourceNotFoundException} with a reindex-specific message. */
    private void getTask(final GetTaskRequest request, final TaskId originalTaskId, final ActionListener<TaskResult> listener) {
        client.admin().cluster().getTask(request, new ActionListener<>() {
            @Override
            public void onResponse(final GetTaskResponse response) {
                listener.onResponse(response.getTask());
            }

            @Override
            public void onFailure(final Exception e) {
                if (e instanceof ResourceNotFoundException) {
                    logger.debug("task [{}] not found, returning as reindex not found", originalTaskId);
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
