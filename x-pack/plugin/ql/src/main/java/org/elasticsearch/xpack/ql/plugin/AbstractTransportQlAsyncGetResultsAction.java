/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.async.AsyncResultsService;
import org.elasticsearch.xpack.core.async.AsyncTaskIndexService;
import org.elasticsearch.xpack.core.async.GetAsyncResultRequest;
import org.elasticsearch.xpack.core.async.StoredAsyncResponse;
import org.elasticsearch.xpack.core.async.StoredAsyncTask;
import org.elasticsearch.xpack.ql.async.AsyncTaskManagementService;

import static org.elasticsearch.xpack.core.ClientHelper.ASYNC_SEARCH_ORIGIN;

public abstract class AbstractTransportQlAsyncGetResultsAction<Response extends ActionResponse, AsyncTask extends StoredAsyncTask<Response>>
    extends HandledTransportAction<GetAsyncResultRequest, Response> {
    private final String actionName;
    private final AsyncResultsService<AsyncTask, StoredAsyncResponse<Response>> resultsService;
    private final TransportService transportService;

    public AbstractTransportQlAsyncGetResultsAction(
        String actionName,
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        NamedWriteableRegistry registry,
        Client client,
        ThreadPool threadPool,
        BigArrays bigArrays,
        Class<? extends AsyncTask> asynkTaskClass
    ) {
        super(actionName, transportService, actionFilters, GetAsyncResultRequest::new);
        this.actionName = actionName;
        this.transportService = transportService;
        this.resultsService = createResultsService(
            transportService,
            clusterService,
            registry,
            client,
            threadPool,
            bigArrays,
            asynkTaskClass
        );
    }

    AsyncResultsService<AsyncTask, StoredAsyncResponse<Response>> createResultsService(
        TransportService transportService,
        ClusterService clusterService,
        NamedWriteableRegistry registry,
        Client client,
        ThreadPool threadPool,
        BigArrays bigArrays,
        Class<? extends AsyncTask> asyncTaskClass
    ) {
        Writeable.Reader<StoredAsyncResponse<Response>> reader = in -> new StoredAsyncResponse<>(responseReader(), in);
        AsyncTaskIndexService<StoredAsyncResponse<Response>> store = new AsyncTaskIndexService<>(
            XPackPlugin.ASYNC_RESULTS_INDEX,
            clusterService,
            threadPool.getThreadContext(),
            client,
            ASYNC_SEARCH_ORIGIN,
            reader,
            registry,
            bigArrays
        );
        return new AsyncResultsService<>(
            store,
            false,
            asyncTaskClass,
            (task, listener, timeout) -> AsyncTaskManagementService.addCompletionListener(threadPool, task, listener, timeout),
            transportService.getTaskManager(),
            clusterService
        );
    }

    @Override
    protected void doExecute(Task task, GetAsyncResultRequest request, ActionListener<Response> listener) {
        DiscoveryNode node = resultsService.getNode(request.getId());
        if (node == null || resultsService.isLocalNode(node)) {
            resultsService.retrieveResult(request, ActionListener.wrap(r -> {
                if (r.getException() != null) {
                    listener.onFailure(r.getException());
                } else {
                    listener.onResponse(r.getResponse());
                }
            }, listener::onFailure));
        } else {
            transportService.sendRequest(
                node,
                actionName,
                request,
                new ActionListenerResponseHandler<>(listener, responseReader(), ThreadPool.Names.SAME)
            );
        }
    }

    public abstract Writeable.Reader<Response> responseReader();
}
