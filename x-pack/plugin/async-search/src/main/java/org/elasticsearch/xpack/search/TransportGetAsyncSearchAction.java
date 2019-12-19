/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.search.action.GetAsyncSearchAction;

import java.io.IOException;

import static org.elasticsearch.xpack.search.AsyncSearchStoreService.ASYNC_SEARCH_INDEX_PREFIX;

public class TransportGetAsyncSearchAction extends HandledTransportAction<GetAsyncSearchAction.Request, AsyncSearchResponse> {
    private static final Logger logger = LogManager.getLogger(TransportGetAsyncSearchAction.class);

    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final TransportService transportService;
    private final AsyncSearchStoreService store;

    @Inject
    public TransportGetAsyncSearchAction(TransportService transportService,
                                         ActionFilters actionFilters,
                                         ClusterService clusterService,
                                         NamedWriteableRegistry registry,
                                         Client client,
                                         ThreadPool threadPool) {
        super(GetAsyncSearchAction.NAME, transportService, actionFilters, GetAsyncSearchAction.Request::new);
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.threadPool = threadPool;
        this.store = new AsyncSearchStoreService(taskManager, threadPool, client, registry);
    }

    @Override
    protected void doExecute(Task task, GetAsyncSearchAction.Request request, ActionListener<AsyncSearchResponse> listener) {
        try {
            AsyncSearchId searchId = AsyncSearchId.decode(request.getId());
            if (searchId.getIndexName().startsWith(ASYNC_SEARCH_INDEX_PREFIX) == false) {
                listener.onFailure(new IllegalArgumentException("invalid id [" + request.getId() + "] that references the wrong index ["
                    + searchId.getIndexName() + "]"));
            }
            if (clusterService.localNode().getId().equals(searchId.getTaskId().getNodeId())) {
                getSearchResponseFromTask(request, searchId, wrapCleanupListener(request, searchId, listener));
            } else {
                TransportRequestOptions.Builder builder = TransportRequestOptions.builder();
                DiscoveryNode node = clusterService.state().nodes().get(searchId.getTaskId().getNodeId());
                if (node == null) {
                    getSearchResponseFromIndex(request, searchId, wrapCleanupListener(request, searchId, listener));
                } else {
                    transportService.sendRequest(node, GetAsyncSearchAction.NAME, request, builder.build(),
                        new ActionListenerResponseHandler<>(listener, AsyncSearchResponse::new, ThreadPool.Names.SAME));
                }
            }
        } catch (IOException e) {
            listener.onFailure(e);
        }
    }

    private void getSearchResponseFromTask(GetAsyncSearchAction.Request request, AsyncSearchId searchId,
                                           ActionListener<AsyncSearchResponse> listener) throws IOException {
        final AsyncSearchTask task = store.getTask(searchId);
        if (task != null) {
            waitForCompletion(request, task, threadPool.relativeTimeInMillis(), listener);
        } else {
            // Task isn't running
            getSearchResponseFromIndex(request, searchId, listener);
        }
    }

   private void getSearchResponseFromIndex(GetAsyncSearchAction.Request request, AsyncSearchId searchId,
                                           ActionListener<AsyncSearchResponse> listener) {
        store.getResponse(searchId, new ActionListener<>() {
                @Override
                public void onResponse(AsyncSearchResponse response) {
                    if (response == null) {
                        // the task failed to store a response but we still have the placeholder in the index so we
                        // force the deletion and throw a resource not found exception.
                        store.deleteResult(searchId, new ActionListener<>() {
                            @Override
                            public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                                listener.onFailure(new ResourceNotFoundException(request.getId() + " not found"));
                            }

                            @Override
                            public void onFailure(Exception exc) {
                                logger.error(() -> new ParameterizedMessage("failed to clean async-search [{}]", request.getId()), exc);
                                listener.onFailure(new ResourceNotFoundException(request.getId() + " not found"));
                            }
                        });
                    } else if (response.getVersion() <= request.getLastVersion()) {
                        // return a not-modified response
                        listener.onResponse(new AsyncSearchResponse(response.id(), response.getVersion(), false));
                    } else {
                        listener.onResponse(response);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
    }

    void waitForCompletion(GetAsyncSearchAction.Request request, AsyncSearchTask task,
                           long startMs, ActionListener<AsyncSearchResponse> listener) {
        final AsyncSearchResponse response = task.getAsyncResponse(false, false);
        if (response == null) {
            // the search task is not fully initialized
            Runnable runnable = threadPool.preserveContext(() -> waitForCompletion(request, task, startMs, listener));
            threadPool.schedule(runnable, TimeValue.timeValueMillis(100), ThreadPool.Names.GENERIC);
        } else {
            try {
                if (response.isRunning() == false) {
                    listener.onResponse(task.getAsyncResponse(true, request.isCleanOnCompletion()));
                } else if (request.getWaitForCompletion().getMillis() < (threadPool.relativeTimeInMillis() - startMs)) {
                    if (response.getVersion() <= request.getLastVersion()) {
                        // return a not-modified response
                        listener.onResponse(new AsyncSearchResponse(response.id(), response.getVersion(), true));
                    } else {
                        listener.onResponse(task.getAsyncResponse(true, request.isCleanOnCompletion()));
                    }
                } else {
                    Runnable runnable = threadPool.preserveContext(() -> waitForCompletion(request, task, startMs, listener));
                    threadPool.schedule(runnable, TimeValue.timeValueMillis(100), ThreadPool.Names.GENERIC);
                }
            } catch (Exception exc) {
                listener.onFailure(exc);
            }
        }
    }

    /**
     * Returns a new listener that delegates the response to another listener and
     * then deletes the async search document from the system index if the response is
     * frozen (because the task has completed, failed or the coordinating node crashed).
     */
    private ActionListener<AsyncSearchResponse> wrapCleanupListener(GetAsyncSearchAction.Request request,
                                                                    AsyncSearchId searchId,
                                                                    ActionListener<AsyncSearchResponse> listener) {
        return ActionListener.wrap(
            resp -> {
                if (request.isCleanOnCompletion() && resp.isRunning() == false) {
                    // TODO: We could ensure that the response was successfully sent to the user
                    //       before deleting, see {@link RestChannel#sendResponse(RestResponse)}.
                    store.deleteResult(searchId, new ActionListener<>() {
                        @Override
                        public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                            listener.onResponse(resp);
                        }

                        @Override
                        public void onFailure(Exception exc) {
                            logger.error(() -> new ParameterizedMessage("failed to clean async-search [{}]", request.getId()), exc);
                            listener.onResponse(resp);
                        }
                    });
                } else {
                    listener.onResponse(resp);
                }
            },
            listener::onFailure
        );
    }
}
