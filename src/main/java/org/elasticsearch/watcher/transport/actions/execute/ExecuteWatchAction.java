/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transport.actions.execute;

import org.elasticsearch.client.Client;
import org.elasticsearch.watcher.client.WatcherAction;

/**
 * This action executes a watch, either ignoring the schedule and condition or just the schedule and can execute a subset of the actions, optionally persisting the history entry
 */
public class ExecuteWatchAction extends WatcherAction<ExecuteWatchRequest, ExecuteWatchResponse, ExecuteWatchRequestBuilder> {

    public static final ExecuteWatchAction INSTANCE = new ExecuteWatchAction();
    public static final String NAME = "cluster:admin/watcher/watch/execute";

    private ExecuteWatchAction() {
        super(NAME);
    }

    @Override
    public ExecuteWatchResponse newResponse() {
        return new ExecuteWatchResponse();
    }

    @Override
    public ExecuteWatchRequestBuilder newRequestBuilder(Client client) {
        return new ExecuteWatchRequestBuilder(client);
    }

}
