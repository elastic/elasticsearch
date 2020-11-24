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
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.core.async.AsyncTask;
import org.elasticsearch.xpack.core.async.AsyncTaskIndexService;
import org.elasticsearch.xpack.core.async.GetAsyncStatusRequest;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.search.action.AsyncStatusResponse;
import org.elasticsearch.xpack.core.search.action.GetAsyncStatusAction;

import java.util.Objects;

import static org.elasticsearch.xpack.core.ClientHelper.ASYNC_SEARCH_ORIGIN;

public class TransportGetAsyncStatusAction extends HandledTransportAction<GetAsyncStatusRequest, AsyncStatusResponse> {
    private final TransportService transportService;
    private final ClusterService clusterService;
    private final AsyncTaskIndexService<AsyncSearchResponse> store;

    @Inject
    public TransportGetAsyncStatusAction(TransportService transportService,
             ActionFilters actionFilters,
             ClusterService clusterService,
             NamedWriteableRegistry registry,
             Client client,
             ThreadPool threadPool) {
        super(GetAsyncStatusAction.NAME, transportService, actionFilters, GetAsyncStatusRequest::new);
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.store = new AsyncTaskIndexService<>(XPackPlugin.ASYNC_RESULTS_INDEX, clusterService,
            threadPool.getThreadContext(), client, ASYNC_SEARCH_ORIGIN, AsyncSearchResponse::new, registry);
    }

    @Override
    protected void doExecute(Task task, GetAsyncStatusRequest request, ActionListener<AsyncStatusResponse> listener) {
        AsyncExecutionId searchId = AsyncExecutionId.decode(request.getId());
        DiscoveryNode node = clusterService.state().nodes().get(searchId.getTaskId().getNodeId());
        if (node == null || Objects.equals(node, clusterService.localNode())) {
            retrieveStatus(request, listener);
        } else {
            transportService.sendRequest(node, GetAsyncStatusAction.NAME, request,
                new ActionListenerResponseHandler<>(listener, AsyncStatusResponse::new, ThreadPool.Names.SAME));
        }
    }

    private void retrieveStatus(GetAsyncStatusRequest request, ActionListener<AsyncStatusResponse> listener) {
        long nowInMillis = System.currentTimeMillis();
        AsyncExecutionId searchId = AsyncExecutionId.decode(request.getId());
        try {
            AsyncTask task = (AsyncTask) taskManager.getTask(searchId.getTaskId().getId());
            if ((task instanceof AsyncSearchTask) && (task.getExecutionId().equals(searchId))) {
                AsyncStatusResponse response = ((AsyncSearchTask) task).getStatusResponse();
                sendFinalResponse(request, response, nowInMillis, listener);
            } else {
                getStatusResponseFromIndex(searchId, request, nowInMillis, listener);
            }
        } catch (Exception exc) {
            listener.onFailure(exc);
        }
    }

    /**
     * Get a status response from index
     */
    private void getStatusResponseFromIndex(AsyncExecutionId searchId,
            GetAsyncStatusRequest request, long nowInMillis, ActionListener<AsyncStatusResponse> listener) {
        store.getStatusResponse(searchId, AsyncStatusResponse::getStatusFromAsyncSearchResponseWithExpirationTime,
            new ActionListener<>() {
                @Override
                public void onResponse(AsyncStatusResponse asyncStatusResponse) {
                    sendFinalResponse(request, asyncStatusResponse, nowInMillis, listener);
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            }
        );
    }

    private static void sendFinalResponse(GetAsyncStatusRequest request,
            AsyncStatusResponse response, long nowInMillis, ActionListener<AsyncStatusResponse> listener) {
        if (response.getExpirationTime() < nowInMillis) { // check if the result has expired
            listener.onFailure(new ResourceNotFoundException(request.getId()));
        } else {
            listener.onResponse(response);
        }
    }
}
