/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher.transport.actions.delete;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.protocol.xpack.watcher.DeleteWatchRequest;
import org.elasticsearch.protocol.xpack.watcher.DeleteWatchResponse;

/**
 * A delete document action request builder.
 */
public class DeleteWatchRequestBuilder extends ActionRequestBuilder<DeleteWatchRequest, DeleteWatchResponse> {

    public DeleteWatchRequestBuilder(ElasticsearchClient client) {
        super(client, DeleteWatchAction.INSTANCE, new DeleteWatchRequest());
    }

    public DeleteWatchRequestBuilder(ElasticsearchClient client, String id) {
        super(client, DeleteWatchAction.INSTANCE, new DeleteWatchRequest(id));
    }

    /**
     * Sets the id of the watch to be deleted
     */
    public DeleteWatchRequestBuilder setId(String id) {
        this.request().setId(id);
        return this;
    }
}
