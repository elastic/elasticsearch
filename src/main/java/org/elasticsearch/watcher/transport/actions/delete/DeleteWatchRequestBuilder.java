/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transport.actions.delete;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.watcher.client.WatcherClient;
import org.elasticsearch.client.Client;

/**
 * A delete document action request builder.
 */
public class DeleteWatchRequestBuilder extends MasterNodeOperationRequestBuilder<DeleteWatchRequest, DeleteWatchResponse, DeleteWatchRequestBuilder, Client> {

    public DeleteWatchRequestBuilder(Client client) {
        super(client, new DeleteWatchRequest());
    }

    public DeleteWatchRequestBuilder(Client client, String id) {
        super(client, new DeleteWatchRequest(id));
    }

    /**
     * Sets the id of the watch to be deleted
     */
    public DeleteWatchRequestBuilder setId(String id) {
        this.request().setId(id);
        return this;
    }

    @Override
    protected void doExecute(final ActionListener<DeleteWatchResponse> listener) {
        new WatcherClient(client).deleteWatch(request, listener);
    }

}
