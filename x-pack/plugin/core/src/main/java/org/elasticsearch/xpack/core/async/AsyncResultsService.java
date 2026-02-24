/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.async;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.QuadFunction;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskManager;

import java.util.Objects;

import static org.elasticsearch.core.Strings.format;

/**
 * Service that is capable of retrieving and cleaning up AsyncTasks regardless of their state. It works with the TaskManager, if a task
 * is still running and AsyncTaskIndexService if task results already stored there.
 */
public class AsyncResultsService<Task extends AsyncTask, Response extends AsyncResponse<Response>> {
    private static final Logger logger = LogManager.getLogger(AsyncResultsService.class);
    private final Class<? extends Task> asyncTaskClass;
    private final TaskManager taskManager;
    private final ClusterService clusterService;
    private final AsyncTaskIndexService<Response> store;
    private final boolean updateInitialResultsInStore;
    private final QuadFunction<Task, ActionListener<Response>, TimeValue, Boolean, Boolean> addCompletionListener;

    /**
     * Creates async results service
     *
     * @param store                       AsyncTaskIndexService for the response we are working with
     * @param updateInitialResultsInStore true if initial results are stored (Async Search) or false otherwise (EQL Search)
     * @param asyncTaskClass              async task class
     * @param addCompletionListener       function that registers a completion listener with the task
     * @param taskManager                 task manager
     * @param clusterService              cluster service
     */
    public AsyncResultsService(
        AsyncTaskIndexService<Response> store,
        boolean updateInitialResultsInStore,
        Class<? extends Task> asyncTaskClass,
        QuadFunction<Task, ActionListener<Response>, TimeValue, Boolean, Boolean> addCompletionListener,
        TaskManager taskManager,
        ClusterService clusterService
    ) {
        this.updateInitialResultsInStore = updateInitialResultsInStore;
        this.asyncTaskClass = asyncTaskClass;
        this.addCompletionListener = addCompletionListener;
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

    public void retrieveResult(GetAsyncResultRequest request, ActionListener<Response> listener) {
        try {
            long nowInMillis = System.currentTimeMillis();
            AsyncExecutionId searchId = AsyncExecutionId.decode(request.getId());
            long expirationTime;
            if (request.getKeepAlive() != null && request.getKeepAlive().getMillis() > 0) {
                expirationTime = nowInMillis + request.getKeepAlive().getMillis();
            } else {
                expirationTime = -1;
            }
            // EQL doesn't store initial or intermediate results so we only need to update expiration time in store for only in case of
            // async search
            if (updateInitialResultsInStore && expirationTime > 0) {
                updateExpirationTime(
                    searchId,
                    expirationTime,
                    listener.delegateFailure((l, unused) -> getSearchResponseFromTask(searchId, request, nowInMillis, expirationTime, l))
                );
            } else {
                getSearchResponseFromTask(searchId, request, nowInMillis, expirationTime, listener);
            }
        } catch (Exception exc) {
            listener.onFailure(exc);
        }
    }

    private void getSearchResponseFromTask(
        AsyncExecutionId searchId,
        GetAsyncResultRequest request,
        long nowInMillis,
        long expirationTimeMillis,
        ActionListener<Response> listener
    ) {
        try {
            final Task task = store.getTaskAndCheckAuthentication(taskManager, searchId, asyncTaskClass);
            if (task == null || (updateInitialResultsInStore && task.isCancelled())) {
                getSearchResponseFromIndexAndUpdateExpiration(searchId, request, nowInMillis, expirationTimeMillis, listener);
                return;
            }

            if (expirationTimeMillis != -1) {
                task.setExpirationTime(expirationTimeMillis);
            }
            boolean added = addCompletionListener.apply(
                task,
                listener.delegateFailure((l, response) -> sendFinalResponse(request, response, nowInMillis, l)),
                request.getWaitForCompletionTimeout(),
                request.getReturnIntermediateResults()
            );
            if (added == false) {
                // the task must have completed, since we cannot add a completion listener
                assert store.getTaskAndCheckAuthentication(taskManager, searchId, asyncTaskClass) == null;
                getSearchResponseFromIndexAndUpdateExpiration(searchId, request, nowInMillis, expirationTimeMillis, listener);
            }
        } catch (Exception exc) {
            listener.onFailure(exc);
        }
    }

    private void getSearchResponseFromIndexAndUpdateExpiration(
        AsyncExecutionId searchId,
        GetAsyncResultRequest request,
        long nowInMillis,
        long expirationTime,
        ActionListener<Response> outListener
    ) {
        var updateListener = outListener.delegateFailure((listener, unused) -> {
            store.getResponse(searchId, true, listener.delegateFailure((l, response) -> {
                try {
                    sendFinalResponse(request, response, nowInMillis, l);
                } finally {
                    if (response instanceof StoredAsyncResponse<?> storedAsyncResponse
                        && storedAsyncResponse.getResponse() instanceof RefCounted refCounted) {
                        refCounted.decRef();
                    }
                }

            }));
        });
        // If updateInitialResultsInStore=false, we can't update expiration while the task is running since the document doesn't exist yet.
        // So let's update the expiration here when the task has been completed.
        if (updateInitialResultsInStore == false && expirationTime != -1) {
            updateExpirationTime(searchId, expirationTime, updateListener.map(unused -> null));
        } else {
            updateListener.onResponse(null);
        }
    }

    private void sendFinalResponse(GetAsyncResultRequest request, Response response, long nowInMillis, ActionListener<Response> listener) {
        // check if the result has expired
        if (response.getExpirationTime() < nowInMillis) {
            listener.onFailure(new ResourceNotFoundException(request.getId()));
            return;
        }

        listener.onResponse(response);
    }

    private void updateExpirationTime(AsyncExecutionId searchId, long expirationTime, ActionListener<UpdateResponse> listener) {
        store.updateExpirationTime(searchId.getDocId(), expirationTime, listener.delegateResponse((l, e) -> {
            RestStatus status = ExceptionsHelper.status(ExceptionsHelper.unwrapCause(e));
            if (status != RestStatus.NOT_FOUND) {
                logger.error(() -> format("failed to update expiration time for async-search [%s]", searchId.getEncoded()), e);
                l.onFailure(e);
            } else {
                // the async search document or its index is not found.
                // That can happen if an invalid/deleted search id is provided.
                l.onFailure(new ResourceNotFoundException(searchId.getEncoded()));
            }
        }));
    }

}
