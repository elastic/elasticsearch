/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.watcher.transport.actions.get;

import org.elasticsearch.action.support.master.MasterNodeReadOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * A delete document action request builder.
 */
public class GetWatchRequestBuilder extends MasterNodeReadOperationRequestBuilder<GetWatchRequest, GetWatchResponse,
        GetWatchRequestBuilder> {

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
