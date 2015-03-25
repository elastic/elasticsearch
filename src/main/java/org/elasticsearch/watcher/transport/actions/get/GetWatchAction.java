/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transport.actions.get;

import org.elasticsearch.watcher.client.WatcherAction;
import org.elasticsearch.client.Client;

/**
 * This action gets an watch by name
 */
public class GetWatchAction extends WatcherAction<GetWatchRequest, GetWatchResponse, GetWatchRequestBuilder> {

    public static final GetWatchAction INSTANCE = new GetWatchAction();
    public static final String NAME = "cluster:monitor/watcher/watch/get";

    private GetWatchAction() {
        super(NAME);
    }

    @Override
    public GetWatchResponse newResponse() {
        return new GetWatchResponse();
    }

    @Override
    public GetWatchRequestBuilder newRequestBuilder(Client client) {
        return new GetWatchRequestBuilder(client);
    }
}
