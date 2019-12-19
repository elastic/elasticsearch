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
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchProgressActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
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
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.search.action.GetAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchRequest;

import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class TransportSubmitAsyncSearchAction extends HandledTransportAction<SubmitAsyncSearchRequest, AsyncSearchResponse> {
    private static final Logger logger = LogManager.getLogger(TransportSubmitAsyncSearchAction.class);

    private final NodeClient nodeClient;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final Supplier<InternalAggregation.ReduceContext> reduceContextSupplier;
    private final TransportSearchAction searchAction;
    private final AsyncSearchStoreService store;
    private final Random random;

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
        this.threadPool = transportService.getThreadPool();
        this.nodeClient = nodeClient;
        this.reduceContextSupplier = () -> searchService.createReduceContext(true);
        this.searchAction = searchAction;
        this.store = new AsyncSearchStoreService(taskManager, threadPool, client, registry);
        this.random = new Random(System.nanoTime());
    }

    @Override
    protected void doExecute(Task task, SubmitAsyncSearchRequest request, ActionListener<AsyncSearchResponse> submitListener) {
        ActionRequestValidationException exc = request.validate();
        if (exc != null) {
            submitListener.onFailure(exc);
            return;
        }

        store.ensureAsyncSearchIndex(clusterService.state(), new ActionListener<>() {
            @Override
            public void onResponse(String indexName) {
                executeSearch(request, indexName, UUIDs.randomBase64UUID(random), submitListener);
            }

            @Override
            public void onFailure(Exception exc) {
                submitListener.onFailure(exc);
            }
        });
    }

    private void executeSearch(SubmitAsyncSearchRequest submitRequest, String indexName, String docID,
                               ActionListener<AsyncSearchResponse> submitListener) {
        final Map<String, String> originHeaders = nodeClient.threadPool().getThreadContext().getHeaders();
        final SearchRequest searchRequest = new SearchRequest(submitRequest.getSearchRequest()) {
            @Override
            public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> taskHeaders) {
                AsyncSearchId searchId = new AsyncSearchId(indexName, docID, new TaskId(nodeClient.getLocalNodeId(), id));
                return new AsyncSearchTask(id, type, action, originHeaders, taskHeaders, searchId, reduceContextSupplier);
            }
        };

        // trigger the async search
        final AtomicReference<Boolean> shouldStoreResult = new AtomicReference<>();
        AsyncSearchTask task = (AsyncSearchTask) taskManager.register("transport", SearchAction.INSTANCE.name(), searchRequest);
        SearchProgressActionListener progressListener = task.getProgressListener();
        searchAction.execute(task, searchRequest,
            new ActionListener<>() {
                @Override
                public void onResponse(SearchResponse response) {
                    progressListener.onResponse(response);
                    logger.debug(() -> new ParameterizedMessage("store async-search [{}]", task.getSearchId().getEncoded()));
                    onTaskCompletion(threadPool, task, shouldStoreResult);
                }

                @Override
                public void onFailure(Exception exc) {
                    progressListener.onFailure(exc);
                    logger.error(() -> new ParameterizedMessage("store failed async-search [{}]", task.getSearchId().getEncoded()), exc);
                    onTaskCompletion(threadPool, task, shouldStoreResult);
                }
            }
        );

        // and get the response asynchronously
        GetAsyncSearchAction.Request getRequest = new GetAsyncSearchAction.Request(task.getSearchId().getEncoded(),
            submitRequest.getWaitForCompletion(), -1, submitRequest.isCleanOnCompletion());
        nodeClient.executeLocally(GetAsyncSearchAction.INSTANCE, getRequest,
            new ActionListener<>() {
                @Override
                public void onResponse(AsyncSearchResponse response) {
                    if (response.isRunning() || submitRequest.isCleanOnCompletion() == false) {
                        // the task is still running and the user cannot wait more so we create
                        // an empty document for further retrieval
                        store.storeInitialResponse(originHeaders, indexName, docID,
                            ActionListener.wrap(() -> {
                                shouldStoreResult.set(true);
                                submitListener.onResponse(response);
                            }));
                    } else {
                        // the user will get a final response directly so no need to store the result on completion
                        shouldStoreResult.set(false);
                        submitListener.onResponse(response);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    // we don't need to store the result if the submit failed
                    shouldStoreResult.set(false);
                    submitListener.onFailure(e);
                }
        });
    }

    private void onTaskCompletion(ThreadPool threadPool, AsyncSearchTask task,
                                    AtomicReference<Boolean> reference) {
        final Boolean shouldStoreResult = reference.get();
        try {
            if (shouldStoreResult == null) {
                // the user is still waiting for a response so we schedule a retry in 100ms
                threadPool.schedule(() -> onTaskCompletion(threadPool, task, reference),
                    TimeValue.timeValueMillis(100), ThreadPool.Names.GENERIC);
            } else if (shouldStoreResult) {
                // the user retrieved an initial partial response so we need to store the final one
                // for further retrieval
                store.storeFinalResponse(task.getOriginHeaders(), task.getAsyncResponse(true, false),
                    new ActionListener<>() {
                        @Override
                        public void onResponse(UpdateResponse updateResponse) {
                            logger.debug(() -> new ParameterizedMessage("store async-search [{}]", task.getSearchId().getEncoded()));
                            taskManager.unregister(task);
                        }

                        @Override
                        public void onFailure(Exception exc) {
                            logger.error(() -> new ParameterizedMessage("failed to store async-search [{}]",
                                task.getSearchId().getEncoded()), exc);
                            taskManager.unregister(task);
                        }
                    });
            } else {
                // the user retrieved the final response already so we don't need to store it
                taskManager.unregister(task);
            }
        } catch (IOException exc) {
            logger.error(() -> new ParameterizedMessage("failed to store async-search [{}]", task.getSearchId().getEncoded()), exc);
            taskManager.unregister(task);
        }
    }
}
