/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.async;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskManager;

import java.util.Objects;
import java.util.function.BiConsumer;

/**
 * Service that is capable of retrieving and cleaning up AsyncTasks regardless of their state. It works with the TaskManager, if a task
 * is still running and AsyncTaskIndexService if task results already stored there.
 */
public class AsyncResultsService<Task extends AsyncTask, Response extends AsyncResponse<Response>> {
    private final Logger logger = LogManager.getLogger(AsyncResultsService.class);
    private final Class<? extends Task> asyncTaskClass;
    private final TaskManager taskManager;
    private final ClusterService clusterService;
    private final AsyncTaskIndexService<Response> store;
    private final boolean updateInitialResultsInStore;
    private final TriConsumer<Task, ActionListener<Response>, TimeValue> addCompletionListener;
    private final BiConsumer<Task, ActionListener<Void>> cancelTask;

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
    public AsyncResultsService(AsyncTaskIndexService<Response> store,
                               boolean updateInitialResultsInStore,
                               Class<? extends Task> asyncTaskClass,
                               TriConsumer<Task, ActionListener<Response>, TimeValue> addCompletionListener,
                               BiConsumer<Task, ActionListener<Void>> cancelTask,
                               TaskManager taskManager,
                               ClusterService clusterService) {
        this.updateInitialResultsInStore = updateInitialResultsInStore;
        this.asyncTaskClass = asyncTaskClass;
        this.addCompletionListener = addCompletionListener;
        this.cancelTask = cancelTask;
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
            if (updateInitialResultsInStore & expirationTime > 0) {
                store.updateExpirationTime(searchId.getDocId(), expirationTime,
                    ActionListener.wrap(
                        p -> getSearchResponseFromTask(searchId, request, nowInMillis, expirationTime, listener),
                        exc -> {
                            //don't log when: the async search document or its index is not found. That can happen if an invalid
                            //search id is provided or no async search initial response has been stored yet.
                            RestStatus status = ExceptionsHelper.status(ExceptionsHelper.unwrapCause(exc));
                            if (status != RestStatus.NOT_FOUND) {
                                logger.error(() -> new ParameterizedMessage("failed to update expiration time for async-search [{}]",
                                    searchId.getEncoded()), exc);
                            }
                            listener.onFailure(new ResourceNotFoundException(searchId.getEncoded()));
                        }
                    ));
            } else {
                getSearchResponseFromTask(searchId, request, nowInMillis, expirationTime, listener);
            }
        } catch (Exception exc) {
            listener.onFailure(exc);
        }
    }

    public void deleteResult(DeleteAsyncResultRequest request, ActionListener<AcknowledgedResponse> listener) {
        try {
            AsyncExecutionId searchId = AsyncExecutionId.decode(request.getId());
            Task task = store.getTask(taskManager, searchId, asyncTaskClass);
            if (task != null) {
                //the task was found and gets cancelled. The response may or may not be found, but we will return 200 anyways.
                cancelTask.accept(task, ActionListener.wrap(
                    (ignore) -> store.deleteResponse(searchId,
                    ActionListener.wrap(
                        r -> listener.onResponse(new AcknowledgedResponse(true)),
                        exc -> {
                            RestStatus status = ExceptionsHelper.status(ExceptionsHelper.unwrapCause(exc));
                            //the index may not be there (no initial async search response stored yet?): we still want to return 200
                            //note that index missing comes back as 200 hence it's handled in the onResponse callback
                            if (status == RestStatus.NOT_FOUND) {
                                listener.onResponse(new AcknowledgedResponse(true));
                            } else {
                                logger.error(() -> new ParameterizedMessage("failed to clean async result [{}]",
                                    searchId.getEncoded()), exc);
                                listener.onFailure(exc);
                            }
                        })),
                    listener::onFailure)
                );
            } else {
                // the task was not found (already cancelled, already completed, or invalid id?)
                // we fail if the response is not found in the index
                ActionListener<DeleteResponse> deleteListener = ActionListener.wrap(
                    resp -> {
                        if (resp.status() == RestStatus.NOT_FOUND) {
                            listener.onFailure(new ResourceNotFoundException(searchId.getEncoded()));
                        } else {
                            listener.onResponse(new AcknowledgedResponse(true));
                        }
                    },
                    exc -> {
                        logger.error(() -> new ParameterizedMessage("failed to clean async-search [{}]", searchId.getEncoded()), exc);
                        listener.onFailure(exc);
                    }
                );
                //we get before deleting to verify that the user is authorized
                store.getResponse(searchId, false,
                    ActionListener.wrap(res -> store.deleteResponse(searchId, deleteListener), listener::onFailure));
            }
        } catch (Exception exc) {
            listener.onFailure(exc);
        }
    }

    private void getSearchResponseFromTask(AsyncExecutionId searchId,
                                           GetAsyncResultRequest request,
                                           long nowInMillis,
                                           long expirationTimeMillis,
                                           ActionListener<Response> listener) {
        try {
            final Task task = store.getTask(taskManager, searchId, asyncTaskClass);
            if (task == null) {
                getSearchResponseFromIndex(searchId, request, nowInMillis, listener);
                return;
            }

            if (task.isCancelled()) {
                listener.onFailure(new ResourceNotFoundException(searchId.getEncoded()));
                return;
            }

            if (expirationTimeMillis != -1) {
                task.setExpirationTime(expirationTimeMillis);
            }
            addCompletionListener.apply(task, new ActionListener<>() {
                @Override
                public void onResponse(Response response) {
                    sendFinalResponse(request, response, nowInMillis, listener);
                }

                @Override
                public void onFailure(Exception exc) {
                    listener.onFailure(exc);
                }
            }, request.getWaitForCompletion());
        } catch (Exception exc) {
            listener.onFailure(exc);
        }
    }

    private void getSearchResponseFromIndex(AsyncExecutionId searchId,
                                            GetAsyncResultRequest request,
                                            long nowInMillis,
                                            ActionListener<Response> listener) {
        store.getResponse(searchId, true,
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

    private void sendFinalResponse(GetAsyncResultRequest request,
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
