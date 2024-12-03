/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.core.async.AsyncStopRequest;
import org.elasticsearch.xpack.core.async.GetAsyncResultRequest;
import org.elasticsearch.xpack.esql.action.EsqlAsyncStopAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;

import java.util.concurrent.TimeUnit;

/**
 * This action will stop running async request and collect the results.
 * If the request is already finished, it will do the same thing as the regular async get.
 */
public class TransportEsqlAsyncStopAction extends HandledTransportAction<AsyncStopRequest, EsqlQueryResponse> {

    private final TransportEsqlQueryAction queryAction;
    private final TransportEsqlAsyncGetResultsAction getResultsAction;
    private final BlockFactory blockFactory;
    private final ClusterService clusterService;
    private final TransportService transportService;

    @Inject
    public TransportEsqlAsyncStopAction(
        TransportService transportService,
        ClusterService clusterService,
        ActionFilters actionFilters,
        TransportEsqlQueryAction queryAction,
        TransportEsqlAsyncGetResultsAction getResultsAction,
        BlockFactory blockFactory
    ) {
        super(EsqlAsyncStopAction.NAME, transportService, actionFilters, AsyncStopRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.queryAction = queryAction;
        this.getResultsAction = getResultsAction;
        this.blockFactory = blockFactory;
        this.transportService = transportService;
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(Task task, AsyncStopRequest request, ActionListener<EsqlQueryResponse> listener) {
        AsyncExecutionId searchId = AsyncExecutionId.decode(request.getId());
        DiscoveryNode node = clusterService.state().nodes().get(searchId.getTaskId().getNodeId());
        if (clusterService.localNode().getId().equals(searchId.getTaskId().getNodeId()) || node == null) {
            // Don't use original request ID here because base64 decoding may not need some padding, but we want to match the original ID
            // for the map lookup
            stopQueryAndReturnResult(task, searchId.getEncoded(), listener);
        } else {
            transportService.sendRequest(
                node,
                EsqlAsyncStopAction.NAME,
                request,
                new ActionListenerResponseHandler<>(listener, EsqlQueryResponse.reader(blockFactory), EsExecutors.DIRECT_EXECUTOR_SERVICE)
            );
        }
    }

    private void stopQueryAndReturnResult(Task task, String asyncId, ActionListener<EsqlQueryResponse> listener) {
        var asyncListener = queryAction.getAsyncListener(asyncId);
        if (asyncListener == null) {
            // This should mean one of the two things: either bad request ID, or the query has already finished
            // In both cases, let regular async get deal with it.
            var getAsyncResultRequest = new GetAsyncResultRequest(asyncId);
            // TODO: this should not be happening, but if the listener is not registered and the query is not finished,
            // we give it some time to finish
            getAsyncResultRequest.setWaitForCompletionTimeout(new TimeValue(1, TimeUnit.SECONDS));
            getResultsAction.execute(task, getAsyncResultRequest, listener);
            return;
        }
        asyncListener.addListener(listener);
        // TODO: send the finish signal to the source
    }
}
