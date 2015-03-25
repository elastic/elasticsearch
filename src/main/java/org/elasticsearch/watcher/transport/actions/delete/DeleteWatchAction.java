/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transport.actions.delete;

import org.elasticsearch.watcher.client.WatcherAction;
import org.elasticsearch.client.Client;

/**
 * This action deletes an watch from in memory, the scheduler and the index
 */
public class DeleteWatchAction extends WatcherAction<DeleteWatchRequest, DeleteWatchResponse, DeleteWatchRequestBuilder> {

    public static final DeleteWatchAction INSTANCE = new DeleteWatchAction();
    public static final String NAME = "cluster:admin/watcher/watch/delete";

    private DeleteWatchAction() {
        super(NAME);
    }

    @Override
    public DeleteWatchResponse newResponse() {
        return new DeleteWatchResponse();
    }

    @Override
    public DeleteWatchRequestBuilder newRequestBuilder(Client client) {
        return new DeleteWatchRequestBuilder(client);
    }
}
