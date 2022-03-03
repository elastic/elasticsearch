/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher.transport.actions.ack;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;

/**
 * A ack watch action request builder.
 */
public class AckWatchRequestBuilder extends ActionRequestBuilder<AckWatchRequest, AckWatchResponse> {

    public AckWatchRequestBuilder(ElasticsearchClient client) {
        super(client, AckWatchAction.INSTANCE, new AckWatchRequest());
    }

    public AckWatchRequestBuilder(ElasticsearchClient client, String id) {
        super(client, AckWatchAction.INSTANCE, new AckWatchRequest(id));
    }

    public AckWatchRequestBuilder setActionIds(String... actionIds) {
        request.setActionIds(actionIds);
        return this;
    }
}
