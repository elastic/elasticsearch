/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.search.action.DeleteAsyncSearchAction;

public class TransportDeleteAsyncSearchAction extends HandledTransportAction<DeleteAsyncSearchAction.Request, AcknowledgedResponse> {
    private final AsyncSearchStoreService store;

    @Inject
    public TransportDeleteAsyncSearchAction(TransportService transportService,
                                            ActionFilters actionFilters,
                                            ThreadPool threadPool,
                                            NamedWriteableRegistry registry,
                                            Client client) {
        super(DeleteAsyncSearchAction.NAME, transportService, actionFilters, DeleteAsyncSearchAction.Request::new);
        this.store = new AsyncSearchStoreService(taskManager, threadPool, client, registry);
    }

    @Override
    protected void doExecute(Task task, DeleteAsyncSearchAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        try {
            AsyncSearchId searchId = AsyncSearchId.decode(request.getId());
            // check if the response can be retrieved by the user (handle security) and then cancel/delete.
            store.getResponse(searchId, ActionListener.wrap(res -> cancelTaskAndDeleteResult(searchId, listener), listener::onFailure));
        } catch (Exception exc) {
            listener.onFailure(exc);
        }
    }

    private void cancelTaskAndDeleteResult(AsyncSearchId searchId, ActionListener<AcknowledgedResponse> listener) {
        store.getClient().admin().cluster().prepareCancelTasks().setTaskId(searchId.getTaskId())
            .execute(ActionListener.wrap(() -> store.deleteResult(searchId, listener)));
    }
}
