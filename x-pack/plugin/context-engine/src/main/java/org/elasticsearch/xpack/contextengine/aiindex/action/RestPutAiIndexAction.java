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
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

@ServerlessScope(Scope.INTERNAL)
public class RestPutAiIndexAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "context_engine_put_ai_index_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.PUT, "/_context_engine/ai_index/{id}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        try (XContentParser parser = request.contentParser()) {
            PutAiIndexAction.Request putRequest = PutAiIndexAction.Request.fromXContent(
                request.param("id"),
                request.paramAsBoolean("create", false),
                parser
            );
            return channel -> client.execute(PutAiIndexAction.INSTANCE, putRequest, new RestToXContentListener<>(channel));
        }
    }
}
