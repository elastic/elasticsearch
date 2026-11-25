/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.ReservingResponseHandler;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportResponse;
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
    private final CircuitBreaker circuitBreaker;

    @SuppressWarnings("this-escape")
    public AbstractTransportQlAsyncGetResultsAction(
        String actionName,
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        NamedWriteableRegistry registry,
        Client client,
        ThreadPool threadPool,
        BigArrays bigArrays,
        CircuitBreakerService circuitBreakerService,
        Class<? extends AsyncTask> asynkTaskClass
    ) {
        // Charges the reply this node serializes for a relayed request until it is written.
        super(
            actionName,
            true,
            transportService,
            actionFilters,
            GetAsyncResultRequest::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            circuitBreakerService.getBreaker(CircuitBreaker.REQUEST)
        );
        this.actionName = actionName;
        this.transportService = transportService;
        this.circuitBreaker = circuitBreakerService.getBreaker(CircuitBreaker.REQUEST);
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
            (task, listener, timeout, returnIntermediateResults) -> AsyncTaskManagementService.addCompletionListener(
                threadPool,
                task,
                listener,
                timeout,
                returnIntermediateResults
            ),
            transportService.getTaskManager(),
            clusterService
        );
    }

    @Override
    protected void doExecute(Task task, GetAsyncResultRequest request, ActionListener<Response> listener) {
        DiscoveryNode node = resultsService.getNode(request.getId());
        if (node == null || resultsService.isLocalNode(node)) {
            resultsService.retrieveResult(request, listener.delegateFailureAndWrap((l, r) -> {
                if (r.getException() != null) {
                    l.onFailure(r.getException());
                } else {
                    l.onResponse(r.getResponse());
                }
            }));
        } else {
            transportService.sendRequest(
                node,
                actionName,
                request,
                new RelayedResponseHandler<>(listener, responseReader(), circuitBreaker, relayedResponseBreakerLabel())
            );
        }
    }

    public abstract Writeable.Reader<Response> responseReader();

    /** Breaker label for a response relayed from the node that runs the query. */
    protected abstract String relayedResponseBreakerLabel();

    /**
     * Charges a response relayed from the node that runs the query in two steps. Its serialized bytes are reserved while it arrives, so
     * one that doesn't fit is rejected before it is read. Once read, the same size is charged again until the listener returns. The REST
     * listeners of these responses build the whole body before they return, so this covers the response for as long as this node holds
     * it. The objects read from it can take several times that size. Either trip reaches the listener as the
     * {@link CircuitBreakingException} itself, which is a 429.
     */
    static final class RelayedResponseHandler<Response extends TransportResponse> extends ReservingResponseHandler<Response> {
        private final CircuitBreaker circuitBreaker;
        private final String label;

        RelayedResponseHandler(
            ActionListener<? super Response> listener,
            Writeable.Reader<Response> reader,
            CircuitBreaker circuitBreaker,
            String label
        ) {
            super(listener, reader, EsExecutors.DIRECT_EXECUTOR_SERVICE, circuitBreaker, label);
            this.circuitBreaker = circuitBreaker;
            this.label = label;
        }

        @Override
        public void handleResponse(Response response) {
            long bytes = reservation.reservedTotal();
            try {
                circuitBreaker.addEstimateBytesAndMaybeBreak(bytes, label);
            } catch (CircuitBreakingException e) {
                listener.onFailure(e);
                return;
            }
            try {
                super.handleResponse(response);
            } finally {
                circuitBreaker.addWithoutBreaking(-bytes, label);
            }
        }
    }
}
