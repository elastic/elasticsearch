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
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.async.AsyncResultsService;
import org.elasticsearch.xpack.core.async.DeleteAsyncResultRequest;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.search.action.DeleteAsyncSearchAction;

import static org.elasticsearch.xpack.search.TransportGetAsyncSearchAction.createResultsService;

public class TransportDeleteAsyncSearchAction extends HandledTransportAction<DeleteAsyncResultRequest, AcknowledgedResponse> {
    private final AsyncResultsService<AsyncSearchTask, AsyncSearchResponse> resultsService;
    private final TransportService transportService;

    @Inject
    public TransportDeleteAsyncSearchAction(TransportService transportService,
                                            ActionFilters actionFilters,
                                            ClusterService clusterService,
                                            ThreadPool threadPool,
                                            NamedWriteableRegistry registry,
                                            Client client) {
        super(DeleteAsyncSearchAction.NAME, transportService, actionFilters, DeleteAsyncResultRequest::new);
        this.transportService = transportService;
        this.resultsService = createResultsService(transportService, clusterService, registry, client, threadPool);
    }

    @Override
    protected void doExecute(Task task, DeleteAsyncResultRequest request, ActionListener<AcknowledgedResponse> listener) {
        DiscoveryNode node = resultsService.getNode(request.getId());
        if (node == null || resultsService.isLocalNode(node)) {
            resultsService.deleteResult(request, listener);
        } else {
            TransportRequestOptions.Builder builder = TransportRequestOptions.builder();
            transportService.sendRequest(node, DeleteAsyncSearchAction.NAME, request, builder.build(),
                new ActionListenerResponseHandler<>(listener, AcknowledgedResponse::new, ThreadPool.Names.SAME));
        }
    }
}
