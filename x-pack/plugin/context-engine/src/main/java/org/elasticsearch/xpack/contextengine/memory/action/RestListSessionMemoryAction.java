/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.contextengine.memory.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.util.List;

@ServerlessScope(Scope.INTERNAL)
public class RestListSessionMemoryAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "context_engine_list_session_memory_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.GET, "/_context_engine/session_memory"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        ListSessionMemoryAction.Request listRequest = new ListSessionMemoryAction.Request(
            request.param("session_id"),
            request.param("user_id"),
            request.param("agent_id"),
            request.param("space"),
            request.param("ai_index_id"),
            request.paramAsInt("size", ListSessionMemoryAction.DEFAULT_SIZE)
        );
        return channel -> client.execute(ListSessionMemoryAction.INSTANCE, listRequest, new RestToXContentListener<>(channel));
    }
}
