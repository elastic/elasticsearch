/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transport.actions.service;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.watcher.client.WatcherClient;
import org.elasticsearch.client.Client;

/**
 */
public class WatcherServiceRequestBuilder extends MasterNodeOperationRequestBuilder<WatcherServiceRequest, WatcherServiceResponse, WatcherServiceRequestBuilder, Client> {

    public WatcherServiceRequestBuilder(Client client) {
        super(client, new WatcherServiceRequest());
    }

    /**
     * Starts the watcher if not already started.
     */
    public WatcherServiceRequestBuilder start() {
        request.start();
        return this;
    }

    /**
     * Stops the watcher if not already stopped.
     */
    public WatcherServiceRequestBuilder stop() {
        request.stop();
        return this;
    }

    /**
     * Starts and stops the watcher.
     */
    public WatcherServiceRequestBuilder restart() {
        request.restart();
        return this;
    }

    @Override
    protected void doExecute(ActionListener<WatcherServiceResponse> listener) {
        new WatcherClient(client).watcherService(request, listener);
    }
}
