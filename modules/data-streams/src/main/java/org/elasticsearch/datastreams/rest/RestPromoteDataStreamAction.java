/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.datastreams.rest;

import org.elasticsearch.action.datastreams.PromoteDataStreamAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestPromoteDataStreamAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "promote_data_stream_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_data_stream/_promote/{name}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        PromoteDataStreamAction.Request request = new PromoteDataStreamAction.Request(restRequest.param("name"));
        return channel -> client.execute(PromoteDataStreamAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
