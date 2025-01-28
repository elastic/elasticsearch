/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support.nodes;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.core.TimeValue;

public abstract class NodesOperationRequestBuilder<
    Request extends BaseNodesRequest,
    Response extends BaseNodesResponse<?>,
    RequestBuilder extends NodesOperationRequestBuilder<Request, Response, RequestBuilder>> extends ActionRequestBuilder<
        Request,
        Response> {

    protected NodesOperationRequestBuilder(ElasticsearchClient client, ActionType<Response> action, Request request) {
        super(client, action, request);
    }

    @SuppressWarnings("unchecked")
    public RequestBuilder setTimeout(TimeValue timeout) {
        request.setTimeout(timeout);
        return (RequestBuilder) this;
    }

}
