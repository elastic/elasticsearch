/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.plugin;

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
import org.elasticsearch.xpack.core.eql.action.AsyncEqlSearchResponse;
import org.elasticsearch.xpack.core.eql.action.GetAsyncEqlSearchAction;
import org.elasticsearch.xpack.eql.action.AsyncEqlSearchTask;

import static org.elasticsearch.xpack.core.ClientHelper.ASYNC_SEARCH_ORIGIN;

public class TransportGetAsyncEqlSearchAction extends HandledTransportAction<GetAsyncEqlSearchAction.Request, AsyncEqlSearchResponse> {
    private final Logger logger = LogManager.getLogger(TransportGetAsyncEqlSearchAction.class);
    private final ClusterService clusterService;
    private final TransportService transportService;
    private final AsyncTaskIndexService<AsyncEqlSearchResponse> store;

    @Inject
    public TransportGetAsyncEqlSearchAction(TransportService transportService,
                                            ActionFilters actionFilters,
                                            ClusterService clusterService,
                                            NamedWriteableRegistry registry,
                                            Client client,
                                            ThreadPool threadPool) {
        super(GetAsyncEqlSearchAction.NAME, transportService, actionFilters, GetAsyncEqlSearchAction.Request::new);
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.store = new AsyncTaskIndexService<>(EqlPlugin.INDEX, clusterService, threadPool.getThreadContext(), client,
            ASYNC_SEARCH_ORIGIN, AsyncEqlSearchResponse::new, registry);
    }

    @Override
    protected void doExecute(Task task, GetAsyncEqlSearchAction.Request request, ActionListener<AsyncEqlSearchResponse> listener) {
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
                transportService.sendRequest(node, GetAsyncEqlSearchAction.NAME, request, builder.build(),
                    new ActionListenerResponseHandler<>(listener, AsyncEqlSearchResponse::new, ThreadPool.Names.SAME));
            }
        } catch (Exception exc) {
            listener.onFailure(exc);
        }
    }

    private void getSearchResponseFromTask(AsyncExecutionId searchId,
                                           GetAsyncEqlSearchAction.Request request,
                                           long nowInMillis,
                                           long expirationTimeMillis,
                                           ActionListener<AsyncEqlSearchResponse> listener) {
        try {
            final AsyncEqlSearchTask task = store.getTask(taskManager, searchId, AsyncEqlSearchTask.class);
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
                public void onResponse(AsyncEqlSearchResponse response) {
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
                                            GetAsyncEqlSearchAction.Request request,
                                            long nowInMillis,
                                            ActionListener<AsyncEqlSearchResponse> listener) {
        store.getResponse(searchId, true,
            new ActionListener<>() {
                @Override
                public void onResponse(AsyncEqlSearchResponse response) {
                    sendFinalResponse(request, response, nowInMillis, listener);
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
    }

    private void sendFinalResponse(GetAsyncEqlSearchAction.Request request,
                                   AsyncEqlSearchResponse response,
                                   long nowInMillis,
                                   ActionListener<AsyncEqlSearchResponse> listener) {
        // check if the result has expired
        if (response.getExpirationTime() < nowInMillis) {
            listener.onFailure(new ResourceNotFoundException(request.getId()));
            return;
        }

        listener.onResponse(response);
    }
}
