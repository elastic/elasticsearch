/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.async.DeleteAsyncResultRequest;
import org.elasticsearch.xpack.core.async.TransportDeleteAsyncResultAction;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

@ServerlessScope(Scope.PUBLIC)
public class RestEsqlDeleteAsyncResultAction extends BaseRestHandler {
    @Override
    public List<RestHandler.Route> routes() {
        return List.of(new RestHandler.Route(DELETE, "/_query/async/{id}"));
    }

    @Override
    public String getName() {
        return "esql_delete_async_result";
    }

    @Override
    protected BaseRestHandler.RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        DeleteAsyncResultRequest delete = new DeleteAsyncResultRequest(request.param("id"));
        return channel -> client.execute(TransportDeleteAsyncResultAction.TYPE, delete, new RestToXContentListener<>(channel));
    }
}
