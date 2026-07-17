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
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * REST handler for the streaming ES|QL query endpoint ({@code POST /_query/stream}).
 * Parses the request body and dispatches to {@link EsqlStreamQueryAction} via
 * {@link EsqlStreamResponseListener}, which streams NDJSON results as pages arrive.
 */
@ServerlessScope(Scope.PUBLIC)
public class RestEsqlStreamQueryAction extends BaseRestHandler {

    public RestEsqlStreamQueryAction() {}

    @Override
    public String getName() {
        return "esql_stream_query";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_query/stream"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        EsqlQueryRequest esqlRequest;
        try (XContentParser parser = request.contentOrSourceParamParser()) {
            esqlRequest = RequestXContent.parseSync(parser);
        }
        return channel -> {
            RestCancellableNodeClient cancellableClient = new RestCancellableNodeClient(client, request.getHttpChannel());
            cancellableClient.execute(EsqlStreamQueryAction.INSTANCE, esqlRequest, new EsqlStreamResponseListener(channel));
        };
    }
}
