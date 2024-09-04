/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.async.AsyncResultsService;
import org.elasticsearch.xpack.core.async.AsyncTaskIndexService;
import org.elasticsearch.xpack.core.async.GetAsyncResultRequest;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.search.action.GetAsyncSearchAction;

import static org.elasticsearch.xpack.core.ClientHelper.ASYNC_SEARCH_ORIGIN;

public class TransportGetAsyncSearchAction extends HandledTransportAction<GetAsyncResultRequest, AsyncSearchResponse> {
    private final AsyncResultsService<AsyncSearchTask, AsyncSearchResponse> resultsService;
    private final TransportService transportService;

    @Inject
    public TransportGetAsyncSearchAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        NamedWriteableRegistry registry,
        Client client,
        ThreadPool threadPool,
        BigArrays bigArrays
    ) {
        super(GetAsyncSearchAction.NAME, transportService, actionFilters, GetAsyncResultRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.transportService = transportService;
        this.resultsService = createResultsService(transportService, clusterService, registry, client, threadPool, bigArrays);
    }

    static AsyncResultsService<AsyncSearchTask, AsyncSearchResponse> createResultsService(
        TransportService transportService,
        ClusterService clusterService,
        NamedWriteableRegistry registry,
        Client client,
        ThreadPool threadPool,
        BigArrays bigArrays
    ) {
        AsyncTaskIndexService<AsyncSearchResponse> store = new AsyncTaskIndexService<>(
            XPackPlugin.ASYNC_RESULTS_INDEX,
            clusterService,
            threadPool.getThreadContext(),
            client,
            ASYNC_SEARCH_ORIGIN,
            AsyncSearchResponse::new,
            registry,
            bigArrays
        );
        return new AsyncResultsService<>(
            store,
            true,
            AsyncSearchTask.class,
            AsyncSearchTask::addCompletionListener,
            transportService.getTaskManager(),
            clusterService
        );
    }

    @Override
    protected void doExecute(Task task, GetAsyncResultRequest request, ActionListener<AsyncSearchResponse> listener) {
        DiscoveryNode node = resultsService.getNode(request.getId());
        if (node == null || resultsService.isLocalNode(node)) {
            resultsService.retrieveResult(request, listener);
        } else {
            transportService.sendRequest(
                node,
                GetAsyncSearchAction.NAME,
                request,
                new ActionListenerResponseHandler<>(listener, AsyncSearchResponse::new, EsExecutors.DIRECT_EXECUTOR_SERVICE)
            );
        }
    }
}
