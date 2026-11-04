/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.contextengine.events.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.util.Arrays;
import java.util.List;

@ServerlessScope(Scope.INTERNAL)
public class RestGetSessionEventsAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "context_engine_get_session_events_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.GET, "/_context_engine/session_events/{session_id}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        GetSessionEventsAction.Request getRequest = new GetSessionEventsAction.Request(
            request.param("session_id"),
            request.param("space"),
            request.hasParam("ai_index_ids") ? Arrays.asList(request.paramAsStringArray("ai_index_ids", null)) : null,
            request.paramAsInt("size", GetSessionEventsAction.DEFAULT_SIZE)
        );
        return channel -> client.execute(GetSessionEventsAction.INSTANCE, getRequest, new RestToXContentListener<>(channel));
    }
}
