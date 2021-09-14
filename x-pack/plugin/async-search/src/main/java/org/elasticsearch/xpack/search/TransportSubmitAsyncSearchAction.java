/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.core.async.AsyncTaskIndexService;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchRequest;

import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.core.ClientHelper.ASYNC_SEARCH_ORIGIN;

public class TransportSubmitAsyncSearchAction extends HandledTransportAction<SubmitAsyncSearchRequest, AsyncSearchResponse> {
    private final NodeClient nodeClient;
    private final Function<SearchRequest, InternalAggregation.ReduceContext> requestToAggReduceContextBuilder;
    private final TransportSearchAction searchAction;
    private final ThreadContext threadContext;
    private final AsyncTaskIndexService<AsyncSearchResponse> store;

    @Inject
    public TransportSubmitAsyncSearchAction(ClusterService clusterService,
                                            TransportService transportService,
                                            ActionFilters actionFilters,
                                            NamedWriteableRegistry registry,
                                            Client client,
                                            NodeClient nodeClient,
                                            SearchService searchService,
                                            TransportSearchAction searchAction,
                                            BigArrays bigArrays) {
        super(SubmitAsyncSearchAction.NAME, transportService, actionFilters, SubmitAsyncSearchRequest::new);
        this.nodeClient = nodeClient;
        this.requestToAggReduceContextBuilder = request -> searchService.aggReduceContextBuilder(request).forFinalReduction();
        this.searchAction = searchAction;
        this.threadContext = transportService.getThreadPool().getThreadContext();
        this.store = new AsyncTaskIndexService<>(XPackPlugin.ASYNC_RESULTS_INDEX, clusterService, threadContext, client,
            ASYNC_SEARCH_ORIGIN, AsyncSearchResponse::new, registry, bigArrays);
    }

    @Override
    protected void doExecute(Task submitTask, SubmitAsyncSearchRequest request, ActionListener<AsyncSearchResponse> submitListener) {
        final SearchRequest searchRequest = createSearchRequest(request, submitTask, request.getKeepAlive());
        AsyncSearchTask searchTask = (AsyncSearchTask) taskManager.register("transport", SearchAction.INSTANCE.name(), searchRequest);
        searchAction.execute(searchTask, searchRequest, searchTask.getSearchProgressActionListener());
        searchTask.addCompletionListener(
            new ActionListener<>() {
                @Override
                public void onResponse(AsyncSearchResponse searchResponse) {
                    if (searchResponse.isRunning() || request.isKeepOnCompletion()) {
                        // the task is still running and the user cannot wait more so we create
                        // a document for further retrieval
                        try {
                            final String docId = searchTask.getExecutionId().getDocId();
                            // creates the fallback response if the node crashes/restarts in the middle of the request
                            // TODO: store intermediate results ?
                            AsyncSearchResponse initialResp = searchResponse.clone(searchResponse.getId());
                            store.createResponse(docId, searchTask.getOriginHeaders(), initialResp,
                                new ActionListener<>() {
                                    @Override
                                    public void onResponse(IndexResponse r) {
                                        if (searchResponse.isRunning()) {
                                            try {
                                                // store the final response on completion unless the submit is cancelled
                                                searchTask.addCompletionListener(
                                                    finalResponse -> onFinalResponse(searchTask, finalResponse, () -> {}));
                                            } finally {
                                                submitListener.onResponse(searchResponse);
                                            }
                                        } else {
                                            onFinalResponse(searchTask, searchResponse, () -> submitListener.onResponse(searchResponse));
                                        }
                                    }

                                    @Override
                                    public void onFailure(Exception exc) {
                                        onFatalFailure(searchTask, exc, searchResponse.isRunning(),
                                            "fatal failure: unable to store initial response", submitListener);
                                    }
                                });
                        } catch (Exception exc) {
                            onFatalFailure(searchTask, exc, searchResponse.isRunning(),
                                "fatal failure: generic error", submitListener);
                        }
                    } else {
                        // the task completed within the timeout so the response is sent back to the user
                        // with a null id since nothing was stored on the cluster.
                        taskManager.unregister(searchTask);
                        submitListener.onResponse(searchResponse.clone(null));
                    }
                }

                @Override
                public void onFailure(Exception exc) {
                    //this will only ever be called if there is an issue scheduling the thread that executes
                    //the completion listener once the wait for completion timeout expires.
                    onFatalFailure(searchTask, exc, true,
                        "fatal failure: addCompletionListener", submitListener);
                }
            }, request.getWaitForCompletionTimeout());
    }

    private SearchRequest createSearchRequest(SubmitAsyncSearchRequest request, Task submitTask, TimeValue keepAlive) {
        String docID = UUIDs.randomBase64UUID();
        Map<String, String> originHeaders = ClientHelper.filterSecurityHeaders(nodeClient.threadPool().getThreadContext().getHeaders());
        SearchRequest searchRequest = new SearchRequest(request.getSearchRequest()) {
            @Override
            public AsyncSearchTask createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> taskHeaders) {
                AsyncExecutionId searchId = new AsyncExecutionId(docID, new TaskId(nodeClient.getLocalNodeId(), id));
                Supplier<InternalAggregation.ReduceContext> aggReduceContextSupplier =
                        () -> requestToAggReduceContextBuilder.apply(request.getSearchRequest());
                return new AsyncSearchTask(id, type, action, parentTaskId, this::buildDescription, keepAlive,
                    originHeaders, taskHeaders, searchId, store.getClientWithOrigin(), nodeClient.threadPool(), aggReduceContextSupplier);
            }
        };
        searchRequest.setParentTask(new TaskId(nodeClient.getLocalNodeId(), submitTask.getId()));
        return searchRequest;
    }

    private void onFatalFailure(AsyncSearchTask task, Exception error, boolean shouldCancel, String cancelReason,
                                ActionListener<AsyncSearchResponse> listener){
        if (shouldCancel && task.isCancelled() == false) {
            task.cancelTask(() -> {
                try {
                    task.addCompletionListener(finalResponse -> taskManager.unregister(task));
                } finally {
                    listener.onFailure(error);
                }
            }, cancelReason);
        } else {
            try {
                task.addCompletionListener(finalResponse -> taskManager.unregister(task));
            } finally {
                listener.onFailure(error);
            }
        }
    }

    private void onFinalResponse(AsyncSearchTask searchTask,
                                 AsyncSearchResponse response,
                                 Runnable nextAction) {
        store.updateResponse(searchTask.getExecutionId().getDocId(),
            threadContext.getResponseHeaders(),
            response,
            ActionListener.wrap(() ->  {
                taskManager.unregister(searchTask);
                nextAction.run();
            })
        );
    }

}
