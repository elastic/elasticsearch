/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.core.async.AsyncTaskIndexService;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.search.action.GetAsyncSearchAction;

import static org.elasticsearch.xpack.core.ClientHelper.ASYNC_SEARCH_ORIGIN;

public class TransportGetAsyncSearchAction extends HandledTransportAction<GetAsyncSearchAction.Request, AsyncSearchResponse> {
    private final Logger logger = LogManager.getLogger(TransportGetAsyncSearchAction.class);
    private final ClusterService clusterService;
    private final TransportService transportService;
    private final AsyncTaskIndexService<AsyncSearchResponse> store;

    @Inject
    public TransportGetAsyncSearchAction(TransportService transportService,
                                         ActionFilters actionFilters,
                                         ClusterService clusterService,
                                         NamedWriteableRegistry registry,
                                         Client client,
                                         ThreadPool threadPool) {
        super(GetAsyncSearchAction.NAME, transportService, actionFilters, GetAsyncSearchAction.Request::new);
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.store = new AsyncTaskIndexService<>(AsyncSearch.INDEX, clusterService, threadPool.getThreadContext(), client,
            ASYNC_SEARCH_ORIGIN, AsyncSearchResponse::new, registry);
    }

    @Override
    protected void doExecute(Task task, GetAsyncSearchAction.Request request, ActionListener<AsyncSearchResponse> listener) {
        try {
            long nowInMillis = System.currentTimeMillis();
            AsyncExecutionId searchId = AsyncExecutionId.decode(request.getId());
            DiscoveryNode node = clusterService.state().nodes().get(searchId.getTaskId().getNodeId());
            if (clusterService.localNode().getId().equals(searchId.getTaskId().getNodeId()) || node == null) {
                if (request.getKeepAlive().getMillis() > 0) {
                    long expirationTime = nowInMillis + request.getKeepAlive().getMillis();
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
                    getSearchResponseFromTask(searchId, request, nowInMillis, -1, listener);
                }
            } else {
                TransportRequestOptions.Builder builder = TransportRequestOptions.builder();
                transportService.sendRequest(node, GetAsyncSearchAction.NAME, request, builder.build(),
                    new ActionListenerResponseHandler<>(listener, AsyncSearchResponse::new, ThreadPool.Names.SAME));
            }
        } catch (Exception exc) {
            listener.onFailure(exc);
        }
    }

    private void getSearchResponseFromTask(AsyncExecutionId searchId,
                                           GetAsyncSearchAction.Request request,
                                           long nowInMillis,
                                           long expirationTimeMillis,
                                           ActionListener<AsyncSearchResponse> listener) {
        try {
            final AsyncSearchTask task = store.getTask(taskManager, searchId, AsyncSearchTask.class);
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
            task.addCompletionListener(new ActionListener<>() {
                @Override
                public void onResponse(AsyncSearchResponse response) {
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
                                           GetAsyncSearchAction.Request request,
                                           long nowInMillis,
                                           ActionListener<AsyncSearchResponse> listener) {
        store.getResponse(searchId, true,
            new ActionListener<>() {
                @Override
                public void onResponse(AsyncSearchResponse response) {
                    sendFinalResponse(request, response, nowInMillis, listener);
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
    }

    private void sendFinalResponse(GetAsyncSearchAction.Request request,
                                   AsyncSearchResponse response,
                                   long nowInMillis,
                                   ActionListener<AsyncSearchResponse> listener) {
        // check if the result has expired
        if (response.getExpirationTime() < nowInMillis) {
            listener.onFailure(new ResourceNotFoundException(request.getId()));
            return;
        }

        listener.onResponse(response);
    }
}
