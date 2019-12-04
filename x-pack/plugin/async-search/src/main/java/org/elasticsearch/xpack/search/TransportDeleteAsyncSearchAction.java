/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.search.action.DeleteAsyncSearchAction;

import java.io.IOException;

public class TransportDeleteAsyncSearchAction extends HandledTransportAction<DeleteAsyncSearchAction.Request, AcknowledgedResponse> {
    private final ClusterService clusterService;
    private final TransportService transportService;
    private final Client client;

    @Inject
    public TransportDeleteAsyncSearchAction(TransportService transportService,
                                            ActionFilters actionFilters,
                                            ClusterService clusterService,
                                            Client client) {
        super(DeleteAsyncSearchAction.NAME, transportService, actionFilters, DeleteAsyncSearchAction.Request::new);
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, DeleteAsyncSearchAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        try {
            AsyncSearchId searchId = AsyncSearchId.decode(request.getId());
            if (clusterService.localNode().getId().equals(searchId.getTaskId().getNodeId())) {
                cancelTaskOnNode(request, searchId, listener);
            } else {
                TransportRequestOptions.Builder builder = TransportRequestOptions.builder();
                DiscoveryNode node = clusterService.state().nodes().get(searchId.getTaskId().getNodeId());
                if (node == null) {
                    deleteResult(request, searchId, listener, false);
                } else {
                    transportService.sendRequest(node, DeleteAsyncSearchAction.NAME, request, builder.build(),
                        new ActionListenerResponseHandler<>(listener, AcknowledgedResponse::new, ThreadPool.Names.SAME));
                }
            }
        } catch (IOException e) {
            listener.onFailure(e);
        }
    }

    private void cancelTaskOnNode(DeleteAsyncSearchAction.Request request, AsyncSearchId searchId,
                                  ActionListener<AcknowledgedResponse> listener) {
        Task runningTask = taskManager.getTask(searchId.getTaskId().getId());
        if (runningTask == null) {
            deleteResult(request, searchId, listener, false);
            return;
        }
        if (runningTask instanceof AsyncSearchTask) {
            AsyncSearchTask searchTask = (AsyncSearchTask) runningTask;
            if (searchTask.getSearchId().equals(request.getId())
                    && searchTask.isCancelled() == false) {
                taskManager.cancel(searchTask, "cancel", () -> {});
                deleteResult(request, searchId, listener, true);
            } else {
                // Task id has been reused by another task due to a node restart
                deleteResult(request, searchId, listener, false);
            }
        } else {
            // Task id has been reused by another task due to a node restart
            deleteResult(request, searchId, listener, false);
        }
    }

    private void deleteResult(DeleteAsyncSearchAction.Request orig, AsyncSearchId searchId,
                              ActionListener<AcknowledgedResponse> next, boolean foundTask) {
        DeleteRequest request = new DeleteRequest(searchId.getIndexName()).id(searchId.getDocId());
        client.delete(request, ActionListener.wrap(
            resp -> {
                if (resp.status() == RestStatus.NOT_FOUND && foundTask == false) {
                    next.onFailure(new ResourceNotFoundException("id [{}] not found", orig.getId()));
                } else {
                    next.onResponse(new AcknowledgedResponse(true));
                }
            },
            exc -> {
                if (foundTask == false) {
                    next.onFailure(new ResourceNotFoundException("id [{}] not found", orig.getId()));
                } else {
                    next.onResponse(new AcknowledgedResponse(true));
                }
            }
        ));
    }
}
