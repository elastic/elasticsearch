/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.single.shard;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.ElasticsearchClient;

public abstract class SingleShardOperationRequestBuilder<Request extends SingleShardRequest<Request>, Response extends ActionResponse,
        RequestBuilder extends SingleShardOperationRequestBuilder<Request, Response, RequestBuilder>>
        extends ActionRequestBuilder<Request, Response> {

    protected SingleShardOperationRequestBuilder(ElasticsearchClient client, ActionType<Response> action, Request request) {
        super(client, action, request);
    }

    /**
     * Sets the index.
     */
    @SuppressWarnings("unchecked")
    public final RequestBuilder setIndex(String index) {
        request.index(index);
        return (RequestBuilder) this;
    }
}
