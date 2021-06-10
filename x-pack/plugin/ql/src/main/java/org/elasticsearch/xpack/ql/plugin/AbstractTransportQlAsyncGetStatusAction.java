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
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.core.async.AsyncTaskIndexService;
import org.elasticsearch.xpack.core.async.GetAsyncStatusRequest;
import org.elasticsearch.xpack.core.async.StoredAsyncResponse;
import org.elasticsearch.xpack.core.async.StoredAsyncTask;
import org.elasticsearch.xpack.ql.async.QlStatusResponse;

import java.util.Objects;

import static org.elasticsearch.xpack.core.ClientHelper.ASYNC_SEARCH_ORIGIN;


public abstract class AbstractTransportQlAsyncGetStatusAction<Response extends ActionResponse & QlStatusResponse.AsyncStatus,
    AsyncTask extends StoredAsyncTask<Response>> extends HandledTransportAction<GetAsyncStatusRequest, QlStatusResponse> {
    private final String actionName;
    private final TransportService transportService;
    private final ClusterService clusterService;
    private final Class<? extends AsyncTask> asyncTaskClass;
    private final AsyncTaskIndexService<StoredAsyncResponse<Response>> store;

    public AbstractTransportQlAsyncGetStatusAction(String actionName,
                                                   TransportService transportService,
                                                   ActionFilters actionFilters,
                                                   ClusterService clusterService,
                                                   NamedWriteableRegistry registry,
                                                   Client client,
                                                   ThreadPool threadPool,
                                                   BigArrays bigArrays,
                                                   Class<? extends AsyncTask> asyncTaskClass) {
        super(actionName, transportService, actionFilters, GetAsyncStatusRequest::new);
        this.actionName = actionName;
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.asyncTaskClass = asyncTaskClass;
        Writeable.Reader<StoredAsyncResponse<Response>> reader = in -> new StoredAsyncResponse<>(responseReader(), in);
        this.store = new AsyncTaskIndexService<>(XPackPlugin.ASYNC_RESULTS_INDEX, clusterService,
            threadPool.getThreadContext(), client, ASYNC_SEARCH_ORIGIN, reader, registry, bigArrays);
    }

    @Override
    protected void doExecute(Task task, GetAsyncStatusRequest request, ActionListener<QlStatusResponse> listener) {
        AsyncExecutionId searchId = AsyncExecutionId.decode(request.getId());
        DiscoveryNode node = clusterService.state().nodes().get(searchId.getTaskId().getNodeId());
        DiscoveryNode localNode = clusterService.state().getNodes().getLocalNode();
        if (node == null || Objects.equals(node, localNode)) {
            store.retrieveStatus(
                request,
                taskManager,
                asyncTaskClass,
                AbstractTransportQlAsyncGetStatusAction::getStatusResponse,
                QlStatusResponse::getStatusFromStoredSearch,
                listener
            );
        } else {
            transportService.sendRequest(node, actionName, request,
                new ActionListenerResponseHandler<>(listener, QlStatusResponse::new, ThreadPool.Names.SAME));
        }
    }

    private static QlStatusResponse getStatusResponse(StoredAsyncTask<?> asyncTask) {
        return new QlStatusResponse(
            asyncTask.getExecutionId().getEncoded(),
            true,
            true,
            asyncTask.getStartTime(),
            asyncTask.getExpirationTimeMillis(),
            null
        );
    }

    protected abstract Writeable.Reader<Response> responseReader();
}
