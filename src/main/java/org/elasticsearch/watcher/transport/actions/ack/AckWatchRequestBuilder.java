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

    public AckWatchRequestBuilder(Client client, String watchName) {
        super(client, new AckWatchRequest(watchName));
    }

    /**
     * Sets the name of the watch to be ack
     */
    public AckWatchRequestBuilder setWatchName(String watchName) {
        this.request().setWatchName(watchName);
        return this;
    }

    @Override
    protected void doExecute(final ActionListener<AckWatchResponse> listener) {
        new WatcherClient(client).ackWatch(request, listener);
    }

}
