/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchRequest;

import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskAction.TASKS_ORIGIN;

public class TransportSubmitAsyncSearchAction extends HandledTransportAction<SubmitAsyncSearchRequest, AsyncSearchResponse> {
    private static final Logger logger = LogManager.getLogger(TransportSubmitAsyncSearchAction.class);

    private final NodeClient nodeClient;
    private final ThreadContext threadContext;
    private final Supplier<ReduceContext> reduceContextSupplier;
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
        this.threadContext= transportService.getThreadPool().getThreadContext();
        this.nodeClient = nodeClient;
        this.reduceContextSupplier = () -> searchService.createReduceContext(true);
        this.searchAction = searchAction;
        this.store = new AsyncSearchStoreService(taskManager, threadContext, client, registry);
    }

    @Override
    protected void doExecute(Task task, SubmitAsyncSearchRequest request, ActionListener<AsyncSearchResponse> submitListener) {
        ActionRequestValidationException errors = request.validate();
        if (errors != null) {
            submitListener.onFailure(errors);
            return;
        }
        CancellableTask submitTask = (CancellableTask) task;
        final String docID = UUIDs.randomBase64UUID();
        final Map<String, String> originHeaders = nodeClient.threadPool().getThreadContext().getHeaders();
        final SearchRequest searchRequest = new SearchRequest(request.getSearchRequest()) {
            @Override
            public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> taskHeaders) {
                AsyncSearchId searchId = new AsyncSearchId(docID, new TaskId(nodeClient.getLocalNodeId(), id));
                return new AsyncSearchTask(id, type, action, parentTaskId, originHeaders, taskHeaders, searchId,
                    nodeClient.threadPool(), reduceContextSupplier);
            }
        };
        searchRequest.setParentTask(new TaskId(nodeClient.getLocalNodeId(), submitTask.getId()));

        // trigger the async search
        AsyncSearchTask searchTask = (AsyncSearchTask) taskManager.register("transport", SearchAction.INSTANCE.name(), searchRequest);
        searchAction.execute(searchTask, searchRequest, searchTask.getProgressListener());
        long expirationTime = System.currentTimeMillis() + request.getKeepAlive().getMillis();
        searchTask.setExpirationTime(expirationTime);
        searchTask.addCompletionListener(searchResponse -> {
            if (searchResponse.isRunning() || request.isCleanOnCompletion() == false) {
                // the task is still running and the user cannot wait more so we create
                // a document for further retrieval
                try {
                    if (submitTask.isCancelled()) {
                        // the user cancelled the submit so we don't store anything
                        // and propagate the failure
                        searchTask.addCompletionListener(finalResponse -> taskManager.unregister(searchTask));
                        submitListener.onFailure(new ElasticsearchException(submitTask.getReasonCancelled()));
                    } else {
                        store.storeInitialResponse(originHeaders, docID, searchResponse,
                            new ActionListener<>() {
                                @Override
                                public void onResponse(IndexResponse r) {
                                    // store the final response on completion unless the submit is cancelled
                                    searchTask.addCompletionListener(finalResponse -> {
                                        if (submitTask.isCancelled()) {
                                            onTaskCompletion(submitTask, searchTask, () -> {});
                                        } else {
                                            storeFinalResponse(submitTask, searchTask, finalResponse);
                                        }
                                    });
                                    submitListener.onResponse(searchResponse);
                                }

                                @Override
                                public void onFailure(Exception exc) {
                                    onTaskFailure(searchTask, exc.getMessage(), () -> {
                                        searchTask.addCompletionListener(finalResponse -> taskManager.unregister(searchTask));
                                        submitListener.onFailure(exc);
                                    });
                                }
                            });
                    }
                } catch (Exception exc) {
                    onTaskFailure(searchTask, exc.getMessage(), () -> {
                        searchTask.addCompletionListener(finalResponse -> taskManager.unregister(searchTask));
                        submitListener.onFailure(exc);
                    });
                }
            } else {
                // the task completed within the timeout so the response is sent back to the user
                // with a null id since nothing was stored on the cluster.
                taskManager.unregister(searchTask);
                submitListener.onResponse(searchResponse.clone(null));
            }
        }, request.getWaitForCompletion());
    }

    private void storeFinalResponse(CancellableTask submitTask, AsyncSearchTask searchTask, AsyncSearchResponse response) {
        try {
            store.storeFinalResponse(response, new ActionListener<>() {
                @Override
                public void onResponse(UpdateResponse updateResponse) {
                    onTaskCompletion(submitTask, searchTask, () -> {});
                }

                @Override
                public void onFailure(Exception exc) {
                    logger.error(() -> new ParameterizedMessage("failed to store async-search [{}]",
                        searchTask.getSearchId().getEncoded()), exc);
                    onTaskCompletion(submitTask, searchTask, () -> {});
                }
            });
        } catch (Exception exc) {
            logger.error(() -> new ParameterizedMessage("failed to store async-search [{}]", searchTask.getSearchId().getEncoded()), exc);
            onTaskCompletion(submitTask, searchTask, () -> {});
        }
    }

    private void onTaskFailure(AsyncSearchTask searchTask, String reason, Runnable onFinish) {
        CancelTasksRequest req = new CancelTasksRequest()
            .setTaskId(new TaskId(nodeClient.getLocalNodeId(), searchTask.getId()))
            .setReason(reason);
        // force the origin to execute the cancellation as a system user
        new OriginSettingClient(nodeClient, TASKS_ORIGIN).admin().cluster().cancelTasks(req, ActionListener.wrap(() -> onFinish.run()));
    }

    private void onTaskCompletion(CancellableTask submitTask, AsyncSearchTask searchTask, Runnable onFinish) {
        if (submitTask.isCancelled()) {
            // the user cancelled the submit so we ensure that there is nothing stored in the response index.
            store.deleteResult(searchTask.getSearchId(),
                ActionListener.wrap(() -> {
                    taskManager.unregister(searchTask);
                    onFinish.run();
                }));
        } else {
            taskManager.unregister(searchTask);
            onFinish.run();
        }
    }
}
