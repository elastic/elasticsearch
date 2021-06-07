/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.datastreams.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.action.PromoteDataStreamAction;

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
