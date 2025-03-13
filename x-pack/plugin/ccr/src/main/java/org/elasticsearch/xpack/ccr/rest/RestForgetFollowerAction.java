/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr.rest;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ccr.action.ForgetFollowerAction;
import org.elasticsearch.xpack.core.ccr.action.ForgetFollowerAction.Request;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestForgetFollowerAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/{index}/_ccr/forget_follower"));
    }

    @Override
    public String getName() {
        return "forget_follower_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest restRequest, final NodeClient client) throws IOException {
        final Request request = createRequest(restRequest, restRequest.param("index"));
        return channel -> client.execute(ForgetFollowerAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }

    private static Request createRequest(final RestRequest restRequest, final String leaderIndex) throws IOException {
        try (XContentParser parser = restRequest.contentOrSourceParamParser()) {
            Request request = Request.fromXContent(parser, leaderIndex);
            if (restRequest.hasParam("timeout")) {
                request.timeout(restRequest.paramAsTime("timeout", null));
            }
            return request;
        }
    }
}
