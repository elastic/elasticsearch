/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.master;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * Base request builder for master node read operations that can be executed on the local node as well
 */
public abstract class MasterNodeReadOperationRequestBuilder<
        Request extends MasterNodeReadRequest<Request>,
        Response extends ActionResponse,
        RequestBuilder extends MasterNodeReadOperationRequestBuilder<Request, Response, RequestBuilder>
    > extends MasterNodeOperationRequestBuilder<Request, Response, RequestBuilder> {

    protected MasterNodeReadOperationRequestBuilder(ElasticsearchClient client, ActionType<Response> action, Request request) {
        super(client, action, request);
    }

    /**
     * Specifies if the request should be executed on local node rather than on master
     */
    @SuppressWarnings("unchecked")
    public final RequestBuilder setLocal(boolean local) {
        request.local(local);
        return (RequestBuilder) this;
    }
}
