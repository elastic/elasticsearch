/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.search.ShardResponseReservation;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.ChildMemoryCircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.core.async.AsyncResultsService;
import org.elasticsearch.xpack.core.async.AsyncTaskIndexService;
import org.elasticsearch.xpack.core.async.GetAsyncResultRequest;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.search.action.GetAsyncSearchAction;

import static org.elasticsearch.xpack.core.ClientHelper.ASYNC_SEARCH_ORIGIN;

public class TransportGetAsyncSearchAction extends HandledTransportAction<GetAsyncResultRequest, AsyncSearchResponse> {

    /** Label for a response from the node that runs the search, which carries hits. */
    static final String RELAYED_RESPONSE_BREAKER_LABEL = ChildMemoryCircuitBreaker.CATEGORY_FETCH + "[async_search]";

    private final AsyncResultsService<AsyncSearchTask, AsyncSearchResponse> resultsService;
    private final TransportService transportService;
    private final ThreadContext threadContext;
    private final CircuitBreaker circuitBreaker;

    @Inject
    public TransportGetAsyncSearchAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        NamedWriteableRegistry registry,
        Client client,
        ThreadPool threadPool,
        BigArrays bigArrays,
        CircuitBreakerService circuitBreakerService
    ) {
        // Charges the response of a forwarded get until it is written.
        super(
            GetAsyncSearchAction.NAME,
            true,
            transportService,
            actionFilters,
            GetAsyncResultRequest::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            circuitBreakerService.getBreaker(CircuitBreaker.REQUEST)
        );
        this.transportService = transportService;
        this.threadContext = threadPool.getThreadContext();
        this.circuitBreaker = circuitBreakerService.getBreaker(CircuitBreaker.REQUEST);
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
        ActionListener<AsyncSearchResponse> listenerWithHeaders = listener.map(response -> {
            threadContext.addResponseHeader(AsyncExecutionId.ASYNC_EXECUTION_IS_RUNNING_HEADER, response.isRunning() ? "?1" : "?0");
            if (response.getId() != null) {
                threadContext.addResponseHeader(AsyncExecutionId.ASYNC_EXECUTION_ID_HEADER, response.getId());
            }
            return response;
        });

        DiscoveryNode node = resultsService.getNode(request.getId());
        if (node == null || resultsService.isLocalNode(node)) {
            resultsService.retrieveResult(request, listenerWithHeaders);
        } else {
            transportService.sendRequest(
                node,
                GetAsyncSearchAction.NAME,
                request,
                new RelayedResponseHandler(listenerWithHeaders, circuitBreaker)
            );
        }
    }

    /**
     * Charges the response of the node that runs the search until it is read, so one that doesn't fit is dropped unread. A trip reaches
     * the listener as the {@link CircuitBreakingException} itself, not as a failure of the search node.
     */
    static final class RelayedResponseHandler extends ActionListenerResponseHandler<AsyncSearchResponse> {
        private final ShardResponseReservation reservation;

        RelayedResponseHandler(ActionListener<? super AsyncSearchResponse> listener, CircuitBreaker circuitBreaker) {
            super(listener, AsyncSearchResponse::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
            this.reservation = new ShardResponseReservation(circuitBreaker, RELAYED_RESPONSE_BREAKER_LABEL, listener::onFailure);
        }

        @Override
        public Releasable reserveResponseBytes(int bytes) {
            return reservation.reserve(bytes);
        }

        @Override
        public void handleException(TransportException e) {
            if (reservation.reportTrip() == false) {
                super.handleException(e);
            }
        }
    }
}
