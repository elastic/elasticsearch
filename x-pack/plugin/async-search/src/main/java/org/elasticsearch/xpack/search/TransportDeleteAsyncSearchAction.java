/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.search.action.DeleteAsyncSearchAction;

import java.io.IOException;

public class TransportDeleteAsyncSearchAction extends HandledTransportAction<DeleteAsyncSearchAction.Request, AcknowledgedResponse> {
    private final NodeClient nodeClient;
    private final AsyncSearchStoreService store;

    @Inject
    public TransportDeleteAsyncSearchAction(TransportService transportService,
                                            ActionFilters actionFilters,
                                            NamedWriteableRegistry registry,
                                            NodeClient nodeClient,
                                            Client client) {
        super(DeleteAsyncSearchAction.NAME, transportService, actionFilters, DeleteAsyncSearchAction.Request::new);
        this.nodeClient = nodeClient;
        this.store = new AsyncSearchStoreService(client, registry);
    }

    @Override
    protected void doExecute(Task task, DeleteAsyncSearchAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        try {
            AsyncSearchId searchId = AsyncSearchId.decode(request.getId());
            // check if the response can be retrieved (handle security)
            store.getResponse(searchId, ActionListener.wrap(res -> cancelTask(searchId, listener), listener::onFailure));
        } catch (IOException exc) {
            listener.onFailure(exc);
        }
    }

    private void cancelTask(AsyncSearchId searchId, ActionListener<AcknowledgedResponse> listener) {
        try {
            nodeClient.execute(CancelTasksAction.INSTANCE, new CancelTasksRequest().setTaskId(searchId.getTaskId()),
                ActionListener.wrap(() -> deleteResult(searchId, listener)));
        } catch (Exception e) {
            deleteResult(searchId, listener);
        }
    }

    private void deleteResult(AsyncSearchId searchId, ActionListener<AcknowledgedResponse> next) {
        DeleteRequest request = new DeleteRequest(searchId.getIndexName()).id(searchId.getDocId());
        store.getClient().delete(request, ActionListener.wrap(
            resp -> {
                if (resp.status() == RestStatus.NOT_FOUND) {
                    next.onFailure(new ResourceNotFoundException("id [{}] not found", searchId.getEncoded()));
                } else {
                    next.onResponse(new AcknowledgedResponse(true));
                }
            },
            exc -> next.onFailure(new ResourceNotFoundException("id [{}] not found", searchId.getEncoded())))
        );
    }
}
