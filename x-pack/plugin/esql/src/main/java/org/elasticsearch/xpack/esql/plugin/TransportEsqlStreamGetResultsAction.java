/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.esql.action.EsqlStreamGetResultAction;
import org.elasticsearch.xpack.esql.streaming.GetStreamResultRequest;
import org.elasticsearch.xpack.esql.streaming.GetStreamResultResponse;
import org.elasticsearch.xpack.esql.streaming.InMemoryStreamStore;
import org.elasticsearch.xpack.esql.streaming.NoopStreamStore;
import org.elasticsearch.xpack.esql.streaming.StreamResultsService;
import org.elasticsearch.xpack.esql.streaming.StreamSearchSecurity;
import org.elasticsearch.xpack.esql.streaming.StreamStorage;

/**
 * Reads a stored stream result on the node that receives the request, the counterpart of {@code TransportEsqlAsyncGetResultsAction}.
 * <p>
 * Unlike the async action it never forwards the request to another node: the result is read from the {@link StreamStorage}, not from
 * the memory of the node that runs the query.
 */
public class TransportEsqlStreamGetResultsAction extends TransportAction<GetStreamResultRequest, GetStreamResultResponse> {

    private final StreamResultsService resultsService;

    @Inject
    @SuppressWarnings("this-escape")
    public TransportEsqlStreamGetResultsAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client
    ) {
        super(EsqlStreamGetResultAction.NAME, actionFilters, transportService.getTaskManager(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.resultsService = createResultsService(clusterService, threadPool, client);
    }

    /**
     * Builds the results service, the way {@code AbstractTransportQlAsyncGetResultsAction#createResultsService} does for async results:
     * the storage, the security check and the service that uses them are created by the action, not registered with the plugin.
     */
    StreamResultsService createResultsService(ClusterService clusterService, ThreadPool threadPool, Client client) {
        return new StreamResultsService(
            createStreamStorage(clusterService, threadPool, client),
            new StreamSearchSecurity(new SecurityContext(clusterService.getSettings(), threadPool.getThreadContext())),
            threadPool
        );
    }

    /**
     * Creates the storage the streams are read from. The system-index backed storage does not exist yet, so this is a
     * {@link NoopStreamStore}, which knows no stream: until it is replaced every stream is not found. This is the one place to change
     * when the real storage exists, and it has the inputs the async storage is built from (cluster service, thread context and client).
     * Tests override it to read from an {@link InMemoryStreamStore}.
     */
    protected StreamStorage createStreamStorage(ClusterService clusterService, ThreadPool threadPool, Client client) {
        return NoopStreamStore.INSTANCE;
    }

    @Override
    protected void doExecute(Task task, GetStreamResultRequest request, ActionListener<GetStreamResultResponse> listener) {
        resultsService.retrieveResult((CancellableTask) task, request, listener);
    }
}
