/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.async;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.tasks.TaskManager;

import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Service that retrieves the status of AsyncTasks regardless of their state.
 * It works with the TaskManager, if a task is still running and
 * AsyncTaskIndexService if task is completed and stored there.
 */
public class AsyncStatusService<Task extends AsyncTask, Response extends AsyncResponse<Response>> {
    private final Class<? extends Task> asyncTaskClass;
    private final TaskManager taskManager;
    private final ClusterService clusterService;
    private final AsyncTaskIndexService<Response> store;
    private final Function<Task, Response> statusFromTaskProducer;
    private final BiFunction<String, Long, Response> completedStatusProducer;

    /**
     * Creates async status service
     *
     * @param store                       AsyncTaskIndexService for the response we are working with
     * @param asyncTaskClass              async task class
     * @param statusFromTaskProducer      function that produces a status response from the given task
     * @param completedStatusProducer     function that produces a status response of the completed task
     *                                          from the given async id and expiration time
     * @param taskManager                 task manager
     * @param clusterService              cluster service
     */
    public AsyncStatusService(AsyncTaskIndexService<Response> store,
            Class<? extends Task> asyncTaskClass,
            Function<Task, Response> statusFromTaskProducer,
            BiFunction<String, Long, Response> completedStatusProducer,
            TaskManager taskManager,
            ClusterService clusterService) {
        this.asyncTaskClass = asyncTaskClass;
        this.statusFromTaskProducer = statusFromTaskProducer;
        this.completedStatusProducer = completedStatusProducer;
        this.taskManager = taskManager;
        this.clusterService = clusterService;
        this.store = store;

    }

    public DiscoveryNode getNode(String id) {
        AsyncExecutionId searchId = AsyncExecutionId.decode(id);
        return clusterService.state().nodes().get(searchId.getTaskId().getNodeId());
    }

    public boolean isLocalNode(DiscoveryNode node) {
        return Objects.requireNonNull(node).equals(clusterService.localNode());
    }

    public void retrieveStatus(GetAsyncStatusRequest request, ActionListener<Response> listener) {
        long nowInMillis = System.currentTimeMillis();
        AsyncExecutionId searchId = AsyncExecutionId.decode(request.getId());
        try {
            final Task task = store.getTaskStatus(taskManager, searchId, asyncTaskClass);
            if (task == null) {
                getStatusResponseFromIndex(searchId, request, nowInMillis, listener);
                return;
            }
            if (task.isCancelled()) {
                listener.onFailure(new ResourceNotFoundException(searchId.getEncoded()));
                return;
            }
            Response response = statusFromTaskProducer.apply(task);
            sendFinalResponse(request, response, nowInMillis, listener);
        } catch (Exception exc) {
            listener.onFailure(exc);
        }
    }

    /**
     * Get a status response from index.
     * We called this function after we were not able to retrieve a task.
     * Since a task is not available, we assume that the async search has completed running.
     */
    private void getStatusResponseFromIndex(AsyncExecutionId searchId,
                                            GetAsyncStatusRequest request,
                                            long nowInMillis,
                                            ActionListener<Response> listener) {
        store.getStatusResponse(searchId, completedStatusProducer,
            new ActionListener<>() {
                @Override
                public void onResponse(Response response) {
                    sendFinalResponse(request, response, nowInMillis, listener);
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
    }

    private void sendFinalResponse(GetAsyncStatusRequest request,
                                   Response response,
                                   long nowInMillis,
                                   ActionListener<Response> listener) {
        // check if the result has expired
        if (response.getExpirationTime() < nowInMillis) {
            listener.onFailure(new ResourceNotFoundException(request.getId()));
            return;
        }
        listener.onResponse(response);
    }
}
