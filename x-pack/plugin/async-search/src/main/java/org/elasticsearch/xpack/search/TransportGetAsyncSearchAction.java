/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

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
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.search.action.GetAsyncSearchAction;

import static org.elasticsearch.action.ActionListener.wrap;

public class TransportGetAsyncSearchAction extends HandledTransportAction<GetAsyncSearchAction.Request, AsyncSearchResponse> {
    private final ClusterService clusterService;
    private final TransportService transportService;
    private final AsyncSearchStoreService store;

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
        this.store = new AsyncSearchStoreService(taskManager, threadPool.getThreadContext(), client, registry);
    }

    @Override
    protected void doExecute(Task task, GetAsyncSearchAction.Request request, ActionListener<AsyncSearchResponse> listener) {
        try {
            AsyncSearchId searchId = AsyncSearchId.decode(request.getId());
            DiscoveryNode node = clusterService.state().nodes().get(searchId.getTaskId().getNodeId());
            if (clusterService.localNode().getId().equals(searchId.getTaskId().getNodeId()) || node == null) {
                if (request.getKeepAlive() != TimeValue.MINUS_ONE) {
                    long expirationTime = System.currentTimeMillis() + request.getKeepAlive().getMillis();
                    store.updateKeepAlive(searchId.getDocId(), expirationTime,
                        wrap(up -> getSearchResponseFromTask(searchId, request, expirationTime, listener), listener::onFailure));
                } else {
                    getSearchResponseFromTask(searchId, request, -1, listener);
                }
            } else {
                TransportRequestOptions.Builder builder = TransportRequestOptions.builder();
                request.setKeepAlive(TimeValue.MINUS_ONE);
                transportService.sendRequest(node, GetAsyncSearchAction.NAME, request, builder.build(),
                    new ActionListenerResponseHandler<>(listener, AsyncSearchResponse::new, ThreadPool.Names.SAME));
            }
        } catch (Exception exc) {
            listener.onFailure(exc);
        }
    }

    private void getSearchResponseFromTask(AsyncSearchId searchId, GetAsyncSearchAction.Request request,
                                           long expirationTimeMillis,
                                           ActionListener<AsyncSearchResponse> listener) {
        try {
            final AsyncSearchTask task = store.getTask(searchId);
            if (task == null) {
                getSearchResponseFromIndex(searchId, request, listener);
                return;
            }

            if (task.isCancelled()) {
                listener.onFailure(new ResourceNotFoundException(searchId.getEncoded() + " not found"));
                return;
            }

            if (expirationTimeMillis != -1) {
                task.setExpirationTime(expirationTimeMillis);
            }
            task.addCompletionListener(response -> {
                if (response.getVersion() <= request.getLastVersion()) {
                    // return a not-modified response
                    listener.onResponse(new AsyncSearchResponse(response.getId(), response.getVersion(),
                        response.isPartial(), response.isRunning(), response.getStartTime(), response.getExpirationTime()));
                } else {
                    listener.onResponse(response);
                }
            }, request.getWaitForCompletion());
        } catch (Exception exc) {
            listener.onFailure(exc);
        }
    }

   private void getSearchResponseFromIndex(AsyncSearchId searchId, GetAsyncSearchAction.Request request,
                                           ActionListener<AsyncSearchResponse> listener) {
        store.getResponse(searchId, new ActionListener<>() {
                @Override
                public void onResponse(AsyncSearchResponse response) {
                    if (response.getVersion() <= request.getLastVersion()) {
                        // return a not-modified response
                        listener.onResponse(new AsyncSearchResponse(response.getId(), response.getVersion(),
                            response.isPartial(), false, response.getStartTime(), response.getExpirationTime()));
                    } else {
                        listener.onResponse(response);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
    }
}
