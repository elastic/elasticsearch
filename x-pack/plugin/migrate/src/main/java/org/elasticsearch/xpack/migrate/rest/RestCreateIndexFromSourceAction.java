/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.migrate.rest;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.migrate.action.CreateIndexFromSourceAction;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class RestCreateIndexFromSourceAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "create_index_from_source_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, "/_create_from/{source}/{dest}"), new Route(POST, "/_create_from/{source}/{dest}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        var createRequest = new CreateIndexFromSourceAction.Request(request.param("source"), request.param("dest"));
        request.applyContentParser(createRequest::fromXContent);
        return channel -> client.execute(CreateIndexFromSourceAction.INSTANCE, createRequest, new RestToXContentListener<>(channel));
    }
}
