/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

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

import java.io.IOException;

public class TransportGetAsyncSearchAction extends HandledTransportAction<GetAsyncSearchAction.Request, AsyncSearchResponse> {
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
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
        this.threadPool = threadPool;
        this.store = new AsyncSearchStoreService(taskManager, threadPool, client, registry);
    }

    @Override
    protected void doExecute(Task task, GetAsyncSearchAction.Request request, ActionListener<AsyncSearchResponse> listener) {
        try {
            AsyncSearchId searchId = AsyncSearchId.decode(request.getId());
            if (clusterService.localNode().getId().equals(searchId.getTaskId().getNodeId())) {
                getSearchResponseFromTask(request, searchId, listener);
            } else {
                TransportRequestOptions.Builder builder = TransportRequestOptions.builder();
                DiscoveryNode node = clusterService.state().nodes().get(searchId.getTaskId().getNodeId());
                if (node == null) {
                    getSearchResponseFromIndex(request, searchId, listener);
                } else {
                    transportService.sendRequest(node, GetAsyncSearchAction.NAME, request, builder.build(),
                        new ActionListenerResponseHandler<>(listener, AsyncSearchResponse::new, ThreadPool.Names.SAME));
                }
            }
        } catch (IOException e) {
            listener.onFailure(e);
        }
    }

    private void getSearchResponseFromTask(GetAsyncSearchAction.Request request, AsyncSearchId searchId,
                                           ActionListener<AsyncSearchResponse> listener) throws IOException {
        final AsyncSearchTask task = store.getTask(searchId);
        if (task != null) {
            waitForCompletion(request, task, threadPool.relativeTimeInMillis(), listener);
        } else {
            // Task isn't running
            getSearchResponseFromIndex(request, searchId, listener);
        }
    }

   private void getSearchResponseFromIndex(GetAsyncSearchAction.Request request, AsyncSearchId searchId,
                                           ActionListener<AsyncSearchResponse> listener) {
        store.getResponse(searchId, new ActionListener<>() {
                @Override
                public void onResponse(AsyncSearchResponse response) {
                    if (response.getVersion() <= request.getLastVersion()) {
                        // return a not-modified response
                        listener.onResponse(new AsyncSearchResponse(response.getId(), response.getVersion(), false));
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

    void waitForCompletion(GetAsyncSearchAction.Request request, AsyncSearchTask task,
                           long startMs, ActionListener<AsyncSearchResponse> listener) {
        final AsyncSearchResponse response = task.getAsyncResponse(false);
        if (response == null) {
            // the search task is not fully initialized
            Runnable runnable = threadPool.preserveContext(() -> waitForCompletion(request, task, startMs, listener));
            threadPool.schedule(runnable, TimeValue.timeValueMillis(100), ThreadPool.Names.GENERIC);
        } else {
            try {
                if (response.isRunning() == false) {
                    listener.onResponse(task.getAsyncResponse(true));
                } else if (request.getWaitForCompletion().getMillis() < (threadPool.relativeTimeInMillis() - startMs)) {
                    if (response.getVersion() <= request.getLastVersion()) {
                        // return a not-modified response
                        listener.onResponse(new AsyncSearchResponse(response.getId(), response.getVersion(), true));
                    } else {
                        listener.onResponse(task.getAsyncResponse(true));
                    }
                } else {
                    Runnable runnable = threadPool.preserveContext(() -> waitForCompletion(request, task, startMs, listener));
                    threadPool.schedule(runnable, TimeValue.timeValueMillis(100), ThreadPool.Names.GENERIC);
                }
            } catch (Exception exc) {
                listener.onFailure(exc);
            }
        }
    }
}
