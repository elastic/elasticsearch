/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.core.async.AsyncTaskIndexService;
import org.elasticsearch.xpack.core.async.GetAsyncStatusRequest;
import org.elasticsearch.xpack.ql.async.QlStatusResponse;
import org.elasticsearch.xpack.ql.async.StoredAsyncResponse;
import org.elasticsearch.xpack.sql.action.SqlQueryResponse;
import org.elasticsearch.xpack.sql.action.SqlQueryTask;

import java.util.Objects;

import static org.elasticsearch.xpack.core.ClientHelper.ASYNC_SEARCH_ORIGIN;


public class TransportSqlAsyncStatusAction extends HandledTransportAction<GetAsyncStatusRequest, QlStatusResponse> {
    private final TransportService transportService;
    private final ClusterService clusterService;
    private final AsyncTaskIndexService<StoredAsyncResponse<SqlQueryResponse>> store;

    @Inject
    public TransportSqlAsyncStatusAction(TransportService transportService,
                                         ActionFilters actionFilters,
                                         ClusterService clusterService,
                                         NamedWriteableRegistry registry,
                                         Client client,
                                         ThreadPool threadPool) {
        super(SqlAsyncStatusAction.NAME, transportService, actionFilters, GetAsyncStatusRequest::new);
        this.transportService = transportService;
        this.clusterService = clusterService;
        Writeable.Reader<StoredAsyncResponse<SqlQueryResponse>> reader = in -> new StoredAsyncResponse<>(SqlQueryResponse::new, in);
        this.store = new AsyncTaskIndexService<>(XPackPlugin.ASYNC_RESULTS_INDEX, clusterService,
            threadPool.getThreadContext(), client, ASYNC_SEARCH_ORIGIN, reader, registry);
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
                SqlQueryTask.class,
                SqlQueryTask::getStatusResponse,
                QlStatusResponse::getStatusFromStoredSearch,
                listener
            );
        } else {
            transportService.sendRequest(node, SqlAsyncStatusAction.NAME, request,
                new ActionListenerResponseHandler<>(listener, QlStatusResponse::new, ThreadPool.Names.SAME));
        }
    }
}
