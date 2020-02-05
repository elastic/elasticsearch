/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchRequest;

import java.util.Map;
import java.util.function.Supplier;

public class TransportSubmitAsyncSearchAction extends HandledTransportAction<SubmitAsyncSearchRequest, AsyncSearchResponse> {
    private static final Logger logger = LogManager.getLogger(TransportSubmitAsyncSearchAction.class);

    private final NodeClient nodeClient;
    private final ThreadContext threadContext;
    private final Supplier<ReduceContext> reduceContextSupplier;
    private final TransportSearchAction searchAction;
    private final AsyncSearchIndexService store;

    @Inject
    public TransportSubmitAsyncSearchAction(ClusterService clusterService,
                                            TransportService transportService,
                                            ActionFilters actionFilters,
                                            NamedWriteableRegistry registry,
                                            Client client,
                                            NodeClient nodeClient,
                                            SearchService searchService,
                                            TransportSearchAction searchAction) {
        super(SubmitAsyncSearchAction.NAME, transportService, actionFilters, SubmitAsyncSearchRequest::new);
        this.threadContext= transportService.getThreadPool().getThreadContext();
        this.nodeClient = nodeClient;
        this.reduceContextSupplier = () -> searchService.createReduceContext(true);
        this.searchAction = searchAction;
        this.store = new AsyncSearchIndexService(clusterService, threadContext, client, registry);
    }

    @Override
    protected void doExecute(Task task, SubmitAsyncSearchRequest request, ActionListener<AsyncSearchResponse> submitListener) {
        ActionRequestValidationException errors = request.validate();
        if (errors != null) {
            submitListener.onFailure(errors);
            return;
        }
        CancellableTask submitTask = (CancellableTask) task;
        final SearchRequest searchRequest = createSearchRequest(request, submitTask.getId(), request.getKeepAlive());
        AsyncSearchTask searchTask = (AsyncSearchTask) taskManager.register("transport", SearchAction.INSTANCE.name(), searchRequest);
        searchAction.execute(searchTask, searchRequest, searchTask.getProgressListener());
        searchTask.addCompletionListener(
            new ActionListener<>() {
                @Override
                public void onResponse(AsyncSearchResponse searchResponse) {
                    if (searchResponse.isRunning() || request.isCleanOnCompletion() == false) {
                        // the task is still running and the user cannot wait more so we create
                        // a document for further retrieval
                        try {
                            if (submitTask.isCancelled()) {
                                // the user cancelled the submit so we don't store anything
                                // and propagate the failure
                                Exception cause = new TaskCancelledException(submitTask.getReasonCancelled());
                                onFatalFailure(searchTask, cause, false, submitListener);
                            } else {
                                final String docId = searchTask.getSearchId().getDocId();
                                store.storeInitialResponse(docId, searchTask.getOriginHeaders(), searchResponse,
                                    new ActionListener<>() {
                                        @Override
                                        public void onResponse(IndexResponse r) {
                                            try {
                                                // store the final response on completion unless the submit is cancelled
                                                searchTask.addCompletionListener(finalResponse ->
                                                    onFinalResponse(submitTask, searchTask, finalResponse));
                                            } finally {
                                                submitListener.onResponse(searchResponse);
                                            }
                                        }

                                        @Override
                                        public void onFailure(Exception exc) {
                                            onFatalFailure(searchTask, exc, searchResponse.isRunning(), submitListener);
                                        }
                                    });
                            }
                        } catch (Exception exc) {
                            onFatalFailure(searchTask, exc, searchResponse.isRunning(), submitListener);
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
                    submitListener.onFailure(exc);
                }
            }, request.getWaitForCompletion());
    }

    private SearchRequest createSearchRequest(SubmitAsyncSearchRequest request, long parentTaskId, TimeValue keepAlive) {
        String docID = UUIDs.randomBase64UUID();
        Map<String, String> originHeaders = nodeClient.threadPool().getThreadContext().getHeaders();
        SearchRequest searchRequest = new SearchRequest(request.getSearchRequest()) {
            @Override
            public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> taskHeaders) {
                AsyncSearchId searchId = new AsyncSearchId(docID, new TaskId(nodeClient.getLocalNodeId(), id));
                return new AsyncSearchTask(id, type, action, parentTaskId, keepAlive, originHeaders, taskHeaders, searchId,
                    store.getClient(), nodeClient.threadPool(), reduceContextSupplier);
            }
        };
        searchRequest.setParentTask(new TaskId(nodeClient.getLocalNodeId(), parentTaskId));
        return searchRequest;
    }

    private void onFatalFailure(AsyncSearchTask task, Exception error, boolean shouldCancel, ActionListener<AsyncSearchResponse> listener) {
        if (shouldCancel) {
            task.cancelTask(() -> {
                try {
                    task.addCompletionListener(finalResponse -> taskManager.unregister(task));
                } finally {
                    listener.onFailure(error);
                }
            });
        } else {
            try {
                task.addCompletionListener(finalResponse -> taskManager.unregister(task));
            } finally {
                listener.onFailure(error);
            }
        }
    }

    private void onFinalResponse(CancellableTask submitTask, AsyncSearchTask searchTask, AsyncSearchResponse response) {
        if (submitTask.isCancelled() || searchTask.isCancelled()) {
            // the user cancelled the submit so we ensure that there is nothing stored in the response index.
            store.deleteResponse(searchTask.getSearchId(), ActionListener.wrap(() -> taskManager.unregister(searchTask)));
            return;
        }

        try {
            store.storeFinalResponse(searchTask.getSearchId().getDocId(), response, new ActionListener<>() {
                @Override
                public void onResponse(UpdateResponse updateResponse) {
                    taskManager.unregister(searchTask);
                }

                @Override
                public void onFailure(Exception exc) {
                    if (exc.getCause() instanceof DocumentMissingException == false) {
                        logger.error(() -> new ParameterizedMessage("failed to store async-search [{}]",
                            searchTask.getSearchId().getEncoded()), exc);
                    }
                    taskManager.unregister(searchTask);
                }
            });
        } catch (Exception exc) {
            logger.error(() -> new ParameterizedMessage("failed to store async-search [{}]", searchTask.getSearchId().getEncoded()), exc);
            taskManager.unregister(searchTask);
        }
    }
}
