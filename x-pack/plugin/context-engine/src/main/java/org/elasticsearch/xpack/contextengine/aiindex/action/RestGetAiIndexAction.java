/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.contextengine.aiindex.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.util.List;

@ServerlessScope(Scope.INTERNAL)
public class RestGetAiIndexAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "context_engine_get_ai_index_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.GET, "/_context_engine/ai_index/{id}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        GetAiIndexAction.Request getRequest = new GetAiIndexAction.Request(request.param("space"), request.param("id"));
        return channel -> client.execute(GetAiIndexAction.INSTANCE, getRequest, new RestToXContentListener<>(channel));
    }
}
