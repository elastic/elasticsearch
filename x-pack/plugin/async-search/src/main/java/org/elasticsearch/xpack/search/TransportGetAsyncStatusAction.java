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
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.async.AsyncStatusService;
import org.elasticsearch.xpack.core.async.AsyncTaskIndexService;
import org.elasticsearch.xpack.core.async.GetAsyncStatusRequest;
import org.elasticsearch.xpack.core.search.action.AsyncStatusResponse;
import org.elasticsearch.xpack.core.search.action.GetAsyncStatusAction;

import static org.elasticsearch.xpack.core.ClientHelper.ASYNC_SEARCH_ORIGIN;

public class TransportGetAsyncStatusAction extends HandledTransportAction<GetAsyncStatusRequest, AsyncStatusResponse> {
    private final TransportService transportService;
    private final AsyncStatusService<AsyncSearchTask, AsyncStatusResponse> statusService;

    @Inject
    public TransportGetAsyncStatusAction(TransportService transportService,
             ActionFilters actionFilters,
             ClusterService clusterService,
             NamedWriteableRegistry registry,
             Client client,
             ThreadPool threadPool) {
        super(GetAsyncStatusAction.NAME, transportService, actionFilters, GetAsyncStatusRequest::new);
        this.transportService = transportService;
        AsyncTaskIndexService<AsyncStatusResponse> store = new AsyncTaskIndexService<>(XPackPlugin.ASYNC_RESULTS_INDEX, clusterService,
            threadPool.getThreadContext(), client, ASYNC_SEARCH_ORIGIN, AsyncStatusResponse::new, registry);
        this.statusService = new AsyncStatusService<>(store, AsyncSearchTask.class, AsyncSearchTask::getStatusResponse,
            AsyncStatusResponse::getCompletedSearchStatusResponse, transportService.getTaskManager(), clusterService);
    }

    @Override
    protected void doExecute(Task task, GetAsyncStatusRequest request, ActionListener<AsyncStatusResponse> listener) {
        DiscoveryNode node = statusService.getNode(request.getId());
        if (node == null || statusService.isLocalNode(node)) {
            statusService.retrieveStatus(request, listener);
        } else {
            TransportRequestOptions.Builder builder = TransportRequestOptions.builder();
            transportService.sendRequest(node, GetAsyncStatusAction.NAME, request, builder.build(),
                new ActionListenerResponseHandler<>(listener, AsyncStatusResponse::new, ThreadPool.Names.SAME));
        }
    }
}
