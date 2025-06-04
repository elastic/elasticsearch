/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.search;

import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.exception.ResourceNotFoundException;
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
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.core.async.AsyncTaskIndexService;
import org.elasticsearch.xpack.core.async.GetAsyncStatusRequest;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.search.action.AsyncStatusResponse;
import org.elasticsearch.xpack.core.search.action.GetAsyncStatusAction;

import java.util.Objects;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.ClientHelper.ASYNC_SEARCH_ORIGIN;
import static org.elasticsearch.xpack.core.async.AsyncTaskIndexService.getTask;

public class TransportGetAsyncStatusAction extends HandledTransportAction<GetAsyncStatusRequest, AsyncStatusResponse> {
    private final TransportService transportService;
    private final ClusterService clusterService;
    private final ThreadContext threadContext;
    private final AsyncTaskIndexService<AsyncSearchResponse> store;

    @Inject
    public TransportGetAsyncStatusAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        NamedWriteableRegistry registry,
        Client client,
        ThreadPool threadPool,
        BigArrays bigArrays
    ) {
        super(GetAsyncStatusAction.NAME, transportService, actionFilters, GetAsyncStatusRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.threadContext = threadPool.getThreadContext();
        this.store = new AsyncTaskIndexService<>(
            XPackPlugin.ASYNC_RESULTS_INDEX,
            clusterService,
            threadPool.getThreadContext(),
            client,
            ASYNC_SEARCH_ORIGIN,
            AsyncSearchResponse::new,
            registry,
            bigArrays
        );
    }

    @Override
    protected void doExecute(Task task, GetAsyncStatusRequest request, ActionListener<AsyncStatusResponse> listener) {
        AsyncExecutionId searchId = AsyncExecutionId.decode(request.getId());
        DiscoveryNode node = clusterService.state().nodes().get(searchId.getTaskId().getNodeId());
        DiscoveryNode localNode = clusterService.state().getNodes().getLocalNode();

        ActionListener<AsyncStatusResponse> listenerWithHeaders = listener.map(response -> {
            threadContext.addResponseHeader(AsyncExecutionId.ASYNC_EXECUTION_IS_RUNNING_HEADER, response.isRunning() ? "?1" : "?0");
            threadContext.addResponseHeader(AsyncExecutionId.ASYNC_EXECUTION_ID_HEADER, response.getId());
            return response;
        });

        if (node == null || Objects.equals(node, localNode)) {
            if (request.getKeepAlive() != null && request.getKeepAlive().getMillis() > 0) {
                long expirationTime = System.currentTimeMillis() + request.getKeepAlive().getMillis();
                store.updateExpirationTime(searchId.getDocId(), expirationTime, ActionListener.wrap(p -> {
                    AsyncSearchTask asyncSearchTask = getTask(taskManager, searchId, AsyncSearchTask.class);
                    if (asyncSearchTask != null) {
                        asyncSearchTask.setExpirationTime(expirationTime);
                    }
                    store.retrieveStatus(
                        request,
                        taskManager,
                        AsyncSearchTask.class,
                        AsyncSearchTask::getStatusResponse,
                        AsyncStatusResponse::getStatusFromStoredSearch,
                        listenerWithHeaders
                    );
                }, exc -> {
                    RestStatus status = ExceptionsHelper.status(ExceptionsHelper.unwrapCause(exc));
                    if (status != RestStatus.NOT_FOUND) {
                        logger.error(() -> format("failed to update expiration time for async-search [%s]", searchId.getEncoded()), exc);
                        listenerWithHeaders.onFailure(exc);
                    } else {
                        // the async search document or its index is not found.
                        // That can happen if an invalid/deleted search id is provided.
                        listenerWithHeaders.onFailure(new ResourceNotFoundException(searchId.getEncoded()));
                    }
                }));
            } else {
                store.retrieveStatus(
                    request,
                    taskManager,
                    AsyncSearchTask.class,
                    AsyncSearchTask::getStatusResponse,
                    AsyncStatusResponse::getStatusFromStoredSearch,
                    listenerWithHeaders
                );
            }
        } else {
            transportService.sendRequest(
                node,
                GetAsyncStatusAction.NAME,
                request,
                new ActionListenerResponseHandler<>(listenerWithHeaders, AsyncStatusResponse::new, EsExecutors.DIRECT_EXECUTOR_SERVICE)
            );
        }
    }
}
