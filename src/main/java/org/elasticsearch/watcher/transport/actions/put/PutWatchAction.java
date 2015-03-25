/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transport.actions.put;

import org.elasticsearch.watcher.client.WatcherAction;
import org.elasticsearch.client.Client;

/**
 * This action puts an watch into the watch index and adds it to the scheduler
 */
public class PutWatchAction extends WatcherAction<PutWatchRequest, PutWatchResponse, PutWatchRequestBuilder> {

    public static final PutWatchAction INSTANCE = new PutWatchAction();
    public static final String NAME = "cluster:admin/watcher/watch/put";

    private PutWatchAction() {
        super(NAME);
    }

    @Override
    public PutWatchRequestBuilder newRequestBuilder(Client client) {
        return new PutWatchRequestBuilder(client);
    }

    @Override
    public PutWatchResponse newResponse() {
        return new PutWatchResponse();
    }
}
