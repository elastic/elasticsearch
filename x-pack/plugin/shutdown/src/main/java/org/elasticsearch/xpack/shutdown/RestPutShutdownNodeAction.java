/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.shutdown;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

public class RestPutShutdownNodeAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "put_shutdown_node";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.PUT, "/_nodes/{nodeId}/shutdown"));
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String nodeId = request.param("nodeId");
        try (XContentParser parser = request.contentParser()) {
            PutShutdownNodeAction.Request parsedRequest = PutShutdownNodeAction.Request.parseRequest(nodeId, parser);

            return channel -> client.execute(PutShutdownNodeAction.INSTANCE, parsedRequest, new RestToXContentListener<>(channel));
        }
    }
}
