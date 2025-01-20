/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.async.DeleteAsyncResultRequest;
import org.elasticsearch.xpack.core.async.TransportDeleteAsyncResultAction;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;
import static org.elasticsearch.xpack.sql.action.Protocol.ID_NAME;
import static org.elasticsearch.xpack.sql.action.Protocol.SQL_ASYNC_DELETE_REST_ENDPOINT;

@ServerlessScope(Scope.PUBLIC)
public class RestSqlAsyncDeleteResultsAction extends BaseRestHandler {
    @Override
    public List<Route> routes() {
        return List.of(new Route(DELETE, SQL_ASYNC_DELETE_REST_ENDPOINT + "{" + ID_NAME + "}"));
    }

    @Override
    public String getName() {
        return "sql_delete_async_result";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        DeleteAsyncResultRequest delete = new DeleteAsyncResultRequest(request.param(ID_NAME));
        return channel -> client.execute(TransportDeleteAsyncResultAction.TYPE, delete, new RestToXContentListener<>(channel));
    }
}
