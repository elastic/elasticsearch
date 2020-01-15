/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.SetOnce;
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
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.search.action.GetAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchRequest;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

public class TransportSubmitAsyncSearchAction extends HandledTransportAction<SubmitAsyncSearchRequest, AsyncSearchResponse> {
    private static final Logger logger = LogManager.getLogger(TransportSubmitAsyncSearchAction.class);

    private final NodeClient nodeClient;
    private final ClusterService clusterService;
    private final ThreadContext threadContext;
    private final Executor generic;
    private final Supplier<InternalAggregation.ReduceContext> reduceContextSupplier;
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
        this.generic = transportService.getThreadPool().generic();
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
                return new AsyncSearchTask(id, type, action, originHeaders, taskHeaders, searchId, reduceContextSupplier);
            }
        };

        // trigger the async search
        final FutureBoolean shouldStoreResult = new FutureBoolean();
        AsyncSearchTask task = (AsyncSearchTask) taskManager.register("transport", SearchAction.INSTANCE.name(), searchRequest);
        SearchProgressActionListener progressListener = task.getProgressListener();
        searchAction.execute(task, searchRequest,
            new ActionListener<>() {
                @Override
                public void onResponse(SearchResponse response) {
                    progressListener.onResponse(response);
                    onFinish();
                }

                @Override
                public void onFailure(Exception exc) {
                    progressListener.onFailure(exc);
                    onFinish();
                }

                private void onFinish() {
                    try {
                        // don't block on a network thread
                        generic.execute(() -> {
                            if (shouldStoreResult.getValue()) {
                                storeTask(task);
                            } else {
                                taskManager.unregister(task);
                            }
                        });
                    } catch (Exception e){
                        taskManager.unregister(task);
                    }
                }
            }
        );

        // and get the response asynchronously
        GetAsyncSearchAction.Request getRequest = new GetAsyncSearchAction.Request(task.getSearchId().getEncoded(),
            submitRequest.getWaitForCompletion(), -1);
        nodeClient.executeLocally(GetAsyncSearchAction.INSTANCE, getRequest,
            new ActionListener<>() {
                @Override
                public void onResponse(AsyncSearchResponse response) {
                    if (response.isRunning() || submitRequest.isCleanOnCompletion() == false) {
                        // the task is still running and the user cannot wait more so we create
                        // an empty document for further retrieval
                        try {
                            store.storeInitialResponse(originHeaders, indexName, docID, response,
                                ActionListener.wrap(() -> {
                                    shouldStoreResult.setValue(true);
                                    submitListener.onResponse(response);
                                }));
                        } catch (Exception exc) {
                            onFailure(exc);
                        }
                    } else {
                        // the user will get a final response directly so no need to store the result on completion
                        shouldStoreResult.setValue(false);
                        submitListener.onResponse(new AsyncSearchResponse(null, response));
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    // we don't need to store the result if the submit failed
                    shouldStoreResult.setValue(false);
                    submitListener.onFailure(e);
                }
        });
    }

    private void storeTask(AsyncSearchTask task) {
        try {
            store.storeFinalResponse(task.getOriginHeaders(), task.getAsyncResponse(0, -1),
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

    /**
     * A condition variable for booleans that notifies consumers when the value is set.
     */
    private static class FutureBoolean {
        final SetOnce<Boolean> value = new SetOnce<>();
        final CountDownLatch latch = new CountDownLatch(1);

        /**
         * Sets the value and notifies the consumers.
         */
        void setValue(boolean v) {
            try {
                value.set(v);
            } finally {
                latch.countDown();
            }
        }

        /**
         * Waits for the value to be set and returns it.
         * Consumers should fork in a different thread to avoid blocking a network thread.
         */
        boolean getValue() {
            try {
                latch.await();
            } catch (InterruptedException e) {
                return false;
            }
            return value.get();
        }
    }
}
