/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher.transport.actions.get;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * A delete document action request builder.
 */
public class GetWatchRequestBuilder extends ActionRequestBuilder<GetWatchRequest, GetWatchResponse> {

    public GetWatchRequestBuilder(ElasticsearchClient client, String id) {
        super(client, GetWatchAction.INSTANCE, new GetWatchRequest(id));
    }


    public GetWatchRequestBuilder(ElasticsearchClient client) {
        super(client, GetWatchAction.INSTANCE, new GetWatchRequest());
    }

    public GetWatchRequestBuilder setId(String id) {
        request.setId(id);
        return this;
    }
}
