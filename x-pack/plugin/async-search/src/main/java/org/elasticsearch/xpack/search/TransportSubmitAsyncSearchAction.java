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
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.tasks.Task;
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
    private final ClusterService clusterService;
    private final ThreadContext threadContext;
    private final Supplier<ReduceContext> reduceContextSupplier;
    private final TransportSearchAction searchAction;
    private final AsyncSearchStoreService store;

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
        this.clusterService = clusterService;
        this.threadContext= transportService.getThreadPool().getThreadContext();
        this.nodeClient = nodeClient;
        this.reduceContextSupplier = () -> searchService.createReduceContext(true);
        this.searchAction = searchAction;
        this.store = new AsyncSearchStoreService(taskManager, threadContext, client, registry);
    }

    @Override
    protected void doExecute(Task task, SubmitAsyncSearchRequest request, ActionListener<AsyncSearchResponse> submitListener) {
        ActionRequestValidationException exc = request.validate();
        if (exc != null) {
            submitListener.onFailure(exc);
            return;
        }
        final String docID = UUIDs.randomBase64UUID();
        store.ensureAsyncSearchIndex(clusterService.state(), ActionListener.wrap(
            indexName -> executeSearch(request, indexName, docID, submitListener),
            submitListener::onFailure
        ));
    }

    private void executeSearch(SubmitAsyncSearchRequest submitRequest, String indexName, String docID,
                               ActionListener<AsyncSearchResponse> submitListener) {
        final Map<String, String> originHeaders = nodeClient.threadPool().getThreadContext().getHeaders();
        final SearchRequest searchRequest = new SearchRequest(submitRequest.getSearchRequest()) {
            @Override
            public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> taskHeaders) {
                AsyncSearchId searchId = new AsyncSearchId(indexName, docID, new TaskId(nodeClient.getLocalNodeId(), id));
                return new AsyncSearchTask(id, type, action, originHeaders, taskHeaders, searchId,
                    nodeClient.threadPool(), reduceContextSupplier);
            }
        };

        // trigger the async search
        AsyncSearchTask task = (AsyncSearchTask) taskManager.register("transport", SearchAction.INSTANCE.name(), searchRequest);
        searchAction.execute(task, searchRequest, task.getProgressListener());
        task.addCompletionListener(searchResponse -> {
            if (searchResponse.isRunning() || submitRequest.isCleanOnCompletion() == false) {
                // the task is still running and the user cannot wait more so we create
                // a document for further retrieval
                try {
                    store.storeInitialResponse(originHeaders, indexName, docID, searchResponse,
                        new ActionListener<>() {
                            @Override
                            public void onResponse(IndexResponse r) {
                                // store the final response
                                task.addCompletionListener(finalResponse -> storeFinalResponse(task, finalResponse));
                                submitListener.onResponse(searchResponse);
                            }

                            @Override
                            public void onFailure(Exception exc) {
                                // TODO: cancel search
                                taskManager.unregister(task);
                                submitListener.onFailure(exc);
                            }
                        });
                } catch (Exception exc) {
                    // TODO: cancel search
                    taskManager.unregister(task);
                    submitListener.onFailure(exc);
                }
            } else {
                // the task completed within the timeout so the response is sent back to the user
                // with a null id since nothing was stored on the cluster.
                taskManager.unregister(task);
                submitListener.onResponse(searchResponse.clone(null));
            }
        }, submitRequest.getWaitForCompletion());
    }

    private void storeFinalResponse(AsyncSearchTask task, AsyncSearchResponse response) {
        try {
            store.storeFinalResponse(task.getOriginHeaders(), response,
                new ActionListener<>() {
                    @Override
                    public void onResponse(UpdateResponse updateResponse) {
                        taskManager.unregister(task);
                    }

                    @Override
                    public void onFailure(Exception exc) {
                        logger.error(() -> new ParameterizedMessage("failed to store async-search [{}]",
                            task.getSearchId().getEncoded()), exc);
                        taskManager.unregister(task);
                    }
                });
        } catch (Exception exc) {
            logger.error(() -> new ParameterizedMessage("failed to store async-search [{}]", task.getSearchId().getEncoded()), exc);
            taskManager.unregister(task);
        }
    }
}
