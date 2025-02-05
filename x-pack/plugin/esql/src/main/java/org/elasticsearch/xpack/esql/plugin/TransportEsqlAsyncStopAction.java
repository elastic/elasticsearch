/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.EsqlRefCountingListener;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.core.async.AsyncSearchSecurity;
import org.elasticsearch.xpack.core.async.AsyncStopRequest;
import org.elasticsearch.xpack.core.async.AsyncTaskIndexService;
import org.elasticsearch.xpack.core.async.GetAsyncResultRequest;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.esql.action.EsqlAsyncStopAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.action.EsqlQueryTask;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.core.ClientHelper.ASYNC_SEARCH_ORIGIN;

/**
 * This action will stop running async request and collect the results.
 * If the request is already finished, it will do the same thing as the regular async get.
 */
public class TransportEsqlAsyncStopAction extends HandledTransportAction<AsyncStopRequest, EsqlQueryResponse> {

    private final TransportEsqlQueryAction queryAction;
    private final TransportEsqlAsyncGetResultsAction getResultsAction;
    private final ExchangeService exchangeService;
    private final BlockFactory blockFactory;
    private final ClusterService clusterService;
    private final TransportService transportService;
    private final AsyncSearchSecurity security;

    @Inject
    public TransportEsqlAsyncStopAction(
        TransportService transportService,
        ClusterService clusterService,
        ActionFilters actionFilters,
        TransportEsqlQueryAction queryAction,
        TransportEsqlAsyncGetResultsAction getResultsAction,
        Client client,
        ExchangeService exchangeService,
        BlockFactory blockFactory
    ) {
        super(EsqlAsyncStopAction.NAME, transportService, actionFilters, AsyncStopRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.queryAction = queryAction;
        this.getResultsAction = getResultsAction;
        this.exchangeService = exchangeService;
        this.blockFactory = blockFactory;
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.security = new AsyncSearchSecurity(
            XPackPlugin.ASYNC_RESULTS_INDEX,
            new SecurityContext(clusterService.getSettings(), client.threadPool().getThreadContext()),
            client,
            ASYNC_SEARCH_ORIGIN
        );
    }

    @Override
    protected void doExecute(Task task, AsyncStopRequest request, ActionListener<EsqlQueryResponse> listener) {
        AsyncExecutionId searchId = AsyncExecutionId.decode(request.getId());
        DiscoveryNode node = clusterService.state().nodes().get(searchId.getTaskId().getNodeId());
        if (clusterService.localNode().getId().equals(searchId.getTaskId().getNodeId()) || node == null) {
            stopQueryAndReturnResult(task, searchId, listener);
        } else {
            transportService.sendRequest(
                node,
                EsqlAsyncStopAction.NAME,
                request,
                new ActionListenerResponseHandler<>(listener, EsqlQueryResponse.reader(blockFactory), EsExecutors.DIRECT_EXECUTOR_SERVICE)
            );
        }
    }

    /**
    * Returns the ID for stored compute session. See {@link TransportEsqlQueryAction#sessionID(Task)}
    */
    private String sessionID(AsyncExecutionId asyncId) {
        return new TaskId(clusterService.localNode().getId(), asyncId.getTaskId().getId()).toString();
    }

    private void stopQueryAndReturnResult(Task task, AsyncExecutionId asyncId, ActionListener<EsqlQueryResponse> listener) {
        String asyncIdStr = asyncId.getEncoded();
        TransportEsqlQueryAction.EsqlQueryListener asyncListener = queryAction.getAsyncListener(asyncIdStr);
        if (asyncListener == null) {
            // This should mean one of the two things: either bad request ID, or the query has already finished
            // In both cases, let regular async get deal with it.
            var getAsyncResultRequest = new GetAsyncResultRequest(asyncIdStr);
            // TODO: this should not be happening, but if the listener is not registered and the query is not finished,
            // we give it some time to finish
            getAsyncResultRequest.setWaitForCompletionTimeout(new TimeValue(1, TimeUnit.SECONDS));
            getResultsAction.execute(task, getAsyncResultRequest, listener);
            return;
        }
        try {
            EsqlQueryTask asyncTask = AsyncTaskIndexService.getTask(taskManager, asyncId, EsqlQueryTask.class);
            if (false == security.currentUserHasAccessToTask(asyncTask)) {
                throw new ResourceNotFoundException(asyncId + " not found");
            }
        } catch (IOException e) {
            throw new ResourceNotFoundException(asyncId + " not found", e);
        }
        // Here we will wait for both the response to become available and for the finish operation to complete
        var responseHolder = new AtomicReference<EsqlQueryResponse>();
        try (var refs = new EsqlRefCountingListener(listener.map(unused -> responseHolder.get()))) {
            asyncListener.addListener(refs.acquire().map(r -> {
                responseHolder.set(r);
                return null;
            }));
            asyncListener.markAsPartial();
            exchangeService.finishSessionEarly(sessionID(asyncId), refs.acquire());
        }
    }
}
