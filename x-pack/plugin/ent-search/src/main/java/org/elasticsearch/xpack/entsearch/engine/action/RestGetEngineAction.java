/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entsearch.engine.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestGetEngineAction extends BaseRestHandler {
    public static final String ENDPOINT = "_engine";

    @Override
    public String getName() {
        return "get_engine";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/" + ENDPOINT + "/{engine_id}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        GetEngineAction.Request request = new GetEngineAction.Request(restRequest.param("engine_id"));
        return channel -> client.execute(GetEngineAction.INSTANCE, request, new RestToXContentListener<GetEngineAction.Response>(channel));
    }
}
