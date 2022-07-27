/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ccr.rest;

import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.PUT;
import static org.elasticsearch.xpack.core.ccr.action.PutFollowAction.INSTANCE;
import static org.elasticsearch.xpack.core.ccr.action.PutFollowAction.Request;

public class RestPutFollowAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, "/{index}/_ccr/follow"));
    }

    @Override
    public String getName() {
        return "ccr_put_follow_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        Request request = createRequest(restRequest);
        return channel -> client.execute(INSTANCE, request, new RestToXContentListener<>(channel));
    }

    private static Request createRequest(RestRequest restRequest) throws IOException {
        try (XContentParser parser = restRequest.contentOrSourceParamParser()) {
            ActiveShardCount waitForActiveShards = ActiveShardCount.parseString(restRequest.param("wait_for_active_shards"));
            return Request.fromXContent(parser, restRequest.param("index"), waitForActiveShards);
        }
    }
}
