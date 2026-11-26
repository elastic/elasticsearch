/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.esql.streaming.DeleteStreamResultRequest;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

/**
 * {@code DELETE /_query/stream/{id}}: cancels the task that writes the results of a stream query to the stream system index if it is still
 * running and deletes what is stored, the counterpart of {@link RestEsqlDeleteAsyncResultAction}. For now nothing is stored, so every id
 * is not found.
 * <p>
 * The id is required and names exactly one stream. There is no route without it, and no pattern or list of ids.
 */
@ServerlessScope(Scope.PUBLIC)
public class RestEsqlDeleteStreamResultAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(DELETE, "/_query/stream/{id}"));
    }

    @Override
    public String getName() {
        return "esql_delete_stream_result";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        DeleteStreamResultRequest delete = parseRequest(request);
        return channel -> client.execute(EsqlStreamDeleteAction.INSTANCE, delete, new RestToXContentListener<>(channel));
    }

    static DeleteStreamResultRequest parseRequest(RestRequest request) {
        return new DeleteStreamResultRequest(request.param("id"));
    }
}
