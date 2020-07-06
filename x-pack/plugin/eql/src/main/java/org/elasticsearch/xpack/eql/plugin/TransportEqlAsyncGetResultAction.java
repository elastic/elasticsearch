/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.plugin;

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
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.async.AsyncResultsService;
import org.elasticsearch.xpack.core.async.AsyncTaskIndexService;
import org.elasticsearch.xpack.core.async.GetAsyncResultRequest;
import org.elasticsearch.xpack.core.eql.EqlAsyncActionNames;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse;
import org.elasticsearch.xpack.eql.action.EqlSearchTask;
import org.elasticsearch.xpack.eql.async.AsyncTaskManagementService;
import org.elasticsearch.xpack.eql.async.StoredAsyncResponse;

import static org.elasticsearch.xpack.core.ClientHelper.ASYNC_SEARCH_ORIGIN;

public class TransportEqlAsyncGetResultAction extends HandledTransportAction<GetAsyncResultRequest, EqlSearchResponse> {
    private final AsyncResultsService<EqlSearchTask, StoredAsyncResponse<EqlSearchResponse>> resultsService;
    private final TransportService transportService;

    @Inject
    public TransportEqlAsyncGetResultAction(TransportService transportService,
                                            ActionFilters actionFilters,
                                            ClusterService clusterService,
                                            NamedWriteableRegistry registry,
                                            Client client,
                                            ThreadPool threadPool) {
        super(EqlAsyncActionNames.EQL_ASYNC_GET_RESULT_ACTION_NAME, transportService, actionFilters, GetAsyncResultRequest::new);
        this.transportService = transportService;
        this.resultsService = createResultsService(transportService, clusterService, registry, client, threadPool);
    }

    static AsyncResultsService<EqlSearchTask, StoredAsyncResponse<EqlSearchResponse>> createResultsService(
        TransportService transportService,
        ClusterService clusterService,
        NamedWriteableRegistry registry,
        Client client,
        ThreadPool threadPool) {
        Writeable.Reader<StoredAsyncResponse<EqlSearchResponse>> reader = in -> new StoredAsyncResponse<>(EqlSearchResponse::new, in);
        AsyncTaskIndexService<StoredAsyncResponse<EqlSearchResponse>> store = new AsyncTaskIndexService<>(XPackPlugin.ASYNC_RESULTS_INDEX,
            clusterService, threadPool.getThreadContext(), client, ASYNC_SEARCH_ORIGIN, reader, registry);
        return new AsyncResultsService<>(store, true, EqlSearchTask.class,
            (task, listener, timeout) -> AsyncTaskManagementService.addCompletionListener(threadPool, task, listener, timeout),
            transportService.getTaskManager(), clusterService);
    }

    @Override
    protected void doExecute(Task task, GetAsyncResultRequest request, ActionListener<EqlSearchResponse> listener) {
        DiscoveryNode node = resultsService.getNode(request.getId());
        if (node == null || resultsService.isLocalNode(node)) {
            resultsService.retrieveResult(request, ActionListener.wrap(
                r -> {
                    if (r.getException() != null) {
                        listener.onFailure(r.getException());
                    } else {
                        listener.onResponse(r.getResponse());
                    }
                },
                listener::onFailure
            ));
        } else {
            TransportRequestOptions.Builder builder = TransportRequestOptions.builder();
            transportService.sendRequest(node, EqlAsyncActionNames.EQL_ASYNC_GET_RESULT_ACTION_NAME, request, builder.build(),
                new ActionListenerResponseHandler<>(listener, EqlSearchResponse::new, ThreadPool.Names.SAME));
        }
    }
}
