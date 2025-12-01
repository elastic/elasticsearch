/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

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
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskResult;
import org.elasticsearch.transport.TransportService;

/**
 * Transport action for getting a reindex task. This validates that the task
 * is actually a reindex task before returning it.
 */
public class TransportGetReindexAction extends HandledTransportAction<GetReindexRequest, GetReindexResponse> {

    public static final ActionType<GetReindexResponse> TYPE = new ActionType<>("cluster:reindex/get");

    private final Client client;

    @Inject
    public TransportGetReindexAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(TYPE.name(), transportService, actionFilters, GetReindexRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.client = client;
    }

    @Override
    protected void doExecute(Task thisTask, GetReindexRequest request, ActionListener<GetReindexResponse> listener) {
        // Use the existing GetTaskAction to retrieve the task
        GetTaskRequest getTaskRequest = new GetTaskRequest();
        getTaskRequest.setTaskId(request.getTaskId());
        getTaskRequest.setWaitForCompletion(request.getWaitForCompletion());
        getTaskRequest.setTimeout(request.getTimeout());

        // TODO: Search on other nodes after task relocation is added
        client.admin().cluster().getTask(getTaskRequest, new ActionListener<>() {
            @Override
            public void onResponse(GetTaskResponse response) {
                TaskResult taskResult = response.getTask();
                if (ReindexAction.NAME.equals(taskResult.getTask().action()) == false) {
                    // Found a matching task by id, but it's not a reindex task, treat it as not found to hide
                    // task implementation details
                    listener.onFailure(notFoundException(request.getTaskId()));
                    return;
                }
                listener.onResponse(new GetReindexResponse(taskResult));
            }

            @Override
            public void onFailure(Exception e) {
                if (e instanceof ResourceNotFoundException) {
                    // Wraps the task not found exception to hide task details
                    listener.onFailure(notFoundException(request.getTaskId()));
                } else {
                    listener.onFailure(e);
                }
            }
        });
    }

    private ResourceNotFoundException notFoundException(TaskId taskId) {
        return new ResourceNotFoundException("Reindex operation [{}] not found", taskId);
    }
}
