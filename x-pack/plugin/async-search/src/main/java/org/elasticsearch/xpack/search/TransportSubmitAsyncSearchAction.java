/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchProgressActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.search.action.GetAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchRequest;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class TransportSubmitAsyncSearchAction extends HandledTransportAction<SubmitAsyncSearchRequest, AsyncSearchResponse> {
    private final NodeClient nodeClient;
    private final Supplier<InternalAggregation.ReduceContext> reduceContextSupplier;
    private final TransportSearchAction searchAction;
    private final AsyncSearchStoreService store;

    @Inject
    public TransportSubmitAsyncSearchAction(TransportService transportService,
                                            ActionFilters actionFilters,
                                            NamedWriteableRegistry registry,
                                            Client client,
                                            NodeClient nodeClient,
                                            SearchService searchService,
                                            TransportSearchAction searchAction) {
        super(SubmitAsyncSearchAction.NAME, transportService, actionFilters, SubmitAsyncSearchRequest::new);
        this.nodeClient = nodeClient;
        this.reduceContextSupplier = () -> searchService.createReduceContext(true);
        this.searchAction = searchAction;
        this.store = new AsyncSearchStoreService(client, registry);
    }

    @Override
    protected void doExecute(Task task, SubmitAsyncSearchRequest request, ActionListener<AsyncSearchResponse> submitListener) {
        Map<String, String> headers = new HashMap<>(nodeClient.threadPool().getThreadContext().getHeaders());

        // add a place holder in the search index and fire the async search
        store.storeInitialResponse(headers,
            ActionListener.wrap(resp -> executeSearch(request, resp, submitListener, headers), submitListener::onFailure));
    }

    private void executeSearch(SubmitAsyncSearchRequest submitRequest, IndexResponse doc,
                               ActionListener<AsyncSearchResponse> submitListener, Map<String, String> originHeaders) {
        final SearchRequest searchRequest = new SearchRequest(submitRequest.getSearchRequest()) {
            @Override
            public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> taskHeaders) {
                AsyncSearchId searchId = new AsyncSearchId(doc.getIndex(), doc.getId(), new TaskId(nodeClient.getLocalNodeId(), id));
                return new AsyncSearchTask(id, type, action, originHeaders, taskHeaders, searchId, reduceContextSupplier);
            }
        };

        AsyncSearchTask task = (AsyncSearchTask) taskManager.register("transport", SearchAction.INSTANCE.name(), searchRequest);
        SearchProgressActionListener progressListener = task.getProgressListener();
        searchAction.execute(task, searchRequest,
            new ActionListener<>() {
                @Override
                public void onResponse(SearchResponse response) {
                    try {
                        progressListener.onResponse(response);
                        store.storeFinalResponse(originHeaders, task.getAsyncResponse(true),
                            ActionListener.wrap(() -> taskManager.unregister(task)));
                    } catch (Exception e) {
                        taskManager.unregister(task);
                    }
                }

                @Override
                public void onFailure(Exception exc) {
                    try {
                        progressListener.onFailure(exc);
                        store.storeFinalResponse(originHeaders, task.getAsyncResponse(true),
                            ActionListener.wrap(() -> taskManager.unregister(task)));
                    } catch (Exception e) {
                        taskManager.unregister(task);
                    }
                }
            }
        );
        GetAsyncSearchAction.Request getRequest = new GetAsyncSearchAction.Request(task.getSearchId().getEncoded(),
            submitRequest.getWaitForCompletion(), -1, submitRequest.isCleanOnCompletion());
        nodeClient.executeLocally(GetAsyncSearchAction.INSTANCE, getRequest, submitListener);
    }
}
