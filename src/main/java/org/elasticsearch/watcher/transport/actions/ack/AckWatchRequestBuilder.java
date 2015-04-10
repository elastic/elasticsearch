/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transport.actions.ack;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.watcher.client.WatcherClient;
import org.elasticsearch.client.Client;

/**
 * A ack watch action request builder.
 */
public class AckWatchRequestBuilder extends MasterNodeOperationRequestBuilder<AckWatchRequest, AckWatchResponse, AckWatchRequestBuilder, Client> {

    public AckWatchRequestBuilder(Client client) {
        super(client, new AckWatchRequest());
    }

    public AckWatchRequestBuilder(Client client, String id) {
        super(client, new AckWatchRequest(id));
    }

    /**
     * Sets the id of the watch to be ack
     */
    public AckWatchRequestBuilder setId(String id) {
        this.request().setId(id);
        return this;
    }

    @Override
    protected void doExecute(final ActionListener<AckWatchResponse> listener) {
        new WatcherClient(client).ackWatch(request, listener);
    }

}
