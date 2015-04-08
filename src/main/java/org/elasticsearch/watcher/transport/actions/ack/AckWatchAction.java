/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transport.actions.ack;

import org.elasticsearch.watcher.client.WatcherAction;
import org.elasticsearch.client.Client;

/**
 * This action acks a watch in memory, and the index
 */
public class AckWatchAction extends WatcherAction<AckWatchRequest, AckWatchResponse, AckWatchRequestBuilder> {

    public static final AckWatchAction INSTANCE = new AckWatchAction();
    public static final String NAME = "cluster:admin/watcher/watch/ack";

    private AckWatchAction() {
        super(NAME);
    }

    @Override
    public AckWatchResponse newResponse() {
        return new AckWatchResponse();
    }

    @Override
    public AckWatchRequestBuilder newRequestBuilder(Client client) {
        return new AckWatchRequestBuilder(client);
    }

}
