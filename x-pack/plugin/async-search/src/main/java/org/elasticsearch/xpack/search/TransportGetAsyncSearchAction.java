/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
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
import org.elasticsearch.xpack.core.search.action.DeleteAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.GetAsyncSearchAction;
import org.elasticsearch.xpack.core.security.authc.Authentication;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.action.ActionListener.wrap;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.AUTHENTICATION_KEY;

public class TransportGetAsyncSearchAction extends HandledTransportAction<GetAsyncSearchAction.Request, AsyncSearchResponse> {
    private Logger logger = LogManager.getLogger(getClass());
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final TransportService transportService;
    private final AsyncSearchStoreService store;
    private final Client client;

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
        this.store = new AsyncSearchStoreService(client, registry);
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, GetAsyncSearchAction.Request request, ActionListener<AsyncSearchResponse> listener) {
        listener = wrapCleanupListener(request, listener);
        try {
            AsyncSearchId searchId = AsyncSearchId.decode(request.getId());
            if (clusterService.localNode().getId().equals(searchId.getTaskId().getNodeId())) {
                getSearchResponseFromTask(request, searchId, listener);
            } else {
                TransportRequestOptions.Builder builder = TransportRequestOptions.builder();
                DiscoveryNode node = clusterService.state().nodes().get(searchId.getTaskId().getNodeId());
                if (node == null) {
                    getSearchResponseFromIndex(request, searchId, listener);
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
        Task runningTask = taskManager.getTask(searchId.getTaskId().getId());
        if (runningTask == null) {
            // Task isn't running
            getSearchResponseFromIndex(request, searchId, listener);
            return;
        }
        if (runningTask instanceof AsyncSearchTask) {
            AsyncSearchTask searchTask = (AsyncSearchTask) runningTask;
            if (searchTask.getSearchId().getEncoded().equals(request.getId()) == false) {
                // Task id has been reused by another task due to a node restart
                getSearchResponseFromIndex(request, searchId, listener);
                return;
            }

            // Check authentication for the user
            final Authentication auth = Authentication.getAuthentication(threadPool.getThreadContext());
            if (ensureAuthenticatedUserIsSame(searchTask.getOriginHeaders(), auth) == false) {
                listener.onFailure(new ResourceNotFoundException(request.getId()));
                return;
            }
            waitForCompletion(request, searchTask, threadPool.relativeTimeInMillis(), listener);
        } else {
            // Task id has been reused by another task due to a node restart
            getSearchResponseFromIndex(request, searchId, listener);
        }
    }

   private void getSearchResponseFromIndex(GetAsyncSearchAction.Request request, AsyncSearchId searchId,
                                           ActionListener<AsyncSearchResponse> listener) {
        store.getResponse(searchId,
            wrap(
                resp -> {
                    if (resp.getVersion() <= request.getLastVersion()) {
                        // return a not-modified response
                        listener.onResponse(new AsyncSearchResponse(resp.id(), resp.getVersion(), false));
                    } else {
                        listener.onResponse(resp);
                    }
                },
                listener::onFailure
            )
        );
    }

    void waitForCompletion(GetAsyncSearchAction.Request request, AsyncSearchTask task,
                           long startMs, ActionListener<AsyncSearchResponse> listener) {
        final AsyncSearchResponse response = task.getAsyncResponse(false);
        try {
            if (response.isRunning() == false) {
                listener.onResponse(response);
            } else if (request.getWaitForCompletion().getMillis() < (threadPool.relativeTimeInMillis() - startMs)) {
                if (response.getVersion() <= request.getLastVersion()) {
                    // return a not-modified response
                    listener.onResponse(new AsyncSearchResponse(response.id(), response.getVersion(), true));
                } else {
                    final AsyncSearchResponse ret = task.getAsyncResponse(true);
                    listener.onResponse(ret);
                }
            } else {
                Runnable runnable = threadPool.preserveContext(() -> waitForCompletion(request, task, startMs, listener));
                threadPool.schedule(runnable, TimeValue.timeValueMillis(100), ThreadPool.Names.GENERIC);
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    static boolean ensureAuthenticatedUserIsSame(Map<String, String> originHeaders, Authentication current) throws IOException {
        if (originHeaders == null || originHeaders.containsKey(AUTHENTICATION_KEY) == false) {
            return true;
        }
        if (current == null) {
            return false;
        }
        Authentication origin = Authentication.decode(originHeaders.get(AUTHENTICATION_KEY));
        return ensureAuthenticatedUserIsSame(origin, current);
    }

    /**
     * Compares the {@link Authentication} that was used to create the {@link AsyncSearchId} with the
     * current authentication.
     */
    static boolean ensureAuthenticatedUserIsSame(Authentication original, Authentication current) {
        final boolean samePrincipal = original.getUser().principal().equals(current.getUser().principal());
        final boolean sameRealmType;
        if (original.getUser().isRunAs()) {
            if (current.getUser().isRunAs()) {
                sameRealmType = original.getLookedUpBy().getType().equals(current.getLookedUpBy().getType());
            }  else {
                sameRealmType = original.getLookedUpBy().getType().equals(current.getAuthenticatedBy().getType());
            }
        } else if (current.getUser().isRunAs()) {
            sameRealmType = original.getAuthenticatedBy().getType().equals(current.getLookedUpBy().getType());
        } else {
            sameRealmType = original.getAuthenticatedBy().getType().equals(current.getAuthenticatedBy().getType());
        }
        return samePrincipal && sameRealmType;
    }

    /**
     * Returns a new listener that delegates the response to another listener and
     * then deletes the async search document from the system index if the response is
     * frozen (because the task has completed, failed or the coordinating node crashed).
     */
    private ActionListener<AsyncSearchResponse> wrapCleanupListener(GetAsyncSearchAction.Request request,
                                                                    ActionListener<AsyncSearchResponse> listener) {
        return ActionListener.wrap(
            resp -> {
                if (request.isCleanOnCompletion() && resp.isRunning() == false) {
                    // TODO: We could ensure that the response was successfully sent to the user
                    //       before deleting, see {@link RestChannel#sendResponse(RestResponse)}.
                    cleanupAsyncSearch(resp, null, listener);
                } else {
                    listener.onResponse(resp);
                }
            },
            exc -> {
                if (request.isCleanOnCompletion() && exc instanceof ResourceNotFoundException == false) {
                    // remove the task on real failures
                    cleanupAsyncSearch(null, exc, listener);
                } else {
                    listener.onFailure(exc);
                }
            }
        );
    }

    private void cleanupAsyncSearch(AsyncSearchResponse resp, Exception exc, ActionListener<AsyncSearchResponse> listener) {
        client.execute(DeleteAsyncSearchAction.INSTANCE, new DeleteAsyncSearchAction.Request(resp.id()),
            ActionListener.wrap(
                () -> {
                    if (exc == null) {
                        listener.onResponse(resp);
                    } else {
                        listener.onFailure(exc);
                    }
            }));
    }
}
