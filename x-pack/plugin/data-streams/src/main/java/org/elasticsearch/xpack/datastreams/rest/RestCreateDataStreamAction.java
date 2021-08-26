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
import org.elasticsearch.xpack.core.action.CreateDataStreamAction;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class RestCreateDataStreamAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "create_data_stream_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, "/_data_stream/{name}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        CreateDataStreamAction.Request putDataStreamRequest = new CreateDataStreamAction.Request(request.param("name"));
        return channel -> client.execute(CreateDataStreamAction.INSTANCE, putDataStreamRequest, new RestToXContentListener<>(channel));
    }
}
