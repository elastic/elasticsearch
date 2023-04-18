/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.async;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackPlugin;

import static org.elasticsearch.xpack.core.ClientHelper.ASYNC_SEARCH_ORIGIN;

public class TransportDeleteAsyncResultAction extends HandledTransportAction<DeleteAsyncResultRequest, AcknowledgedResponse> {
    private final DeleteAsyncResultsService deleteResultsService;
    private final ClusterService clusterService;
    private final TransportService transportService;

    @Inject
    public TransportDeleteAsyncResultAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        NamedWriteableRegistry registry,
        Client client,
        ThreadPool threadPool,
        BigArrays bigArrays
    ) {
        super(DeleteAsyncResultAction.NAME, transportService, actionFilters, DeleteAsyncResultRequest::new);
        this.transportService = transportService;
        this.clusterService = clusterService;
        AsyncTaskIndexService<?> store = new AsyncTaskIndexService<>(
            XPackPlugin.ASYNC_RESULTS_INDEX,
            clusterService,
            threadPool.getThreadContext(),
            client,
            ASYNC_SEARCH_ORIGIN,
            (in) -> {
                throw new UnsupportedOperationException("Reading is not supported during deletion");
            },
            registry,
            bigArrays
        );
        this.deleteResultsService = new DeleteAsyncResultsService(store, transportService.getTaskManager());
    }

    @Override
    protected void doExecute(Task task, DeleteAsyncResultRequest request, ActionListener<AcknowledgedResponse> listener) {
        AsyncExecutionId searchId = AsyncExecutionId.decode(request.getId());
        DiscoveryNode node = clusterService.state().nodes().get(searchId.getTaskId().getNodeId());
        if (clusterService.localNode().getId().equals(searchId.getTaskId().getNodeId()) || node == null) {
            deleteResultsService.deleteResponse(request, listener);
        } else {
            transportService.sendRequest(
                node,
                DeleteAsyncResultAction.NAME,
                request,
                new ActionListenerResponseHandler<>(listener, AcknowledgedResponse::readFrom, ThreadPool.Names.SAME)
            );
        }
    }
}
