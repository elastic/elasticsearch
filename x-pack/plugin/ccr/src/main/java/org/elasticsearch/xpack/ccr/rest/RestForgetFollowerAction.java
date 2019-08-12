/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ccr.action.ForgetFollowerAction;
import org.elasticsearch.xpack.core.ccr.action.ForgetFollowerAction.Request;

import java.io.IOException;
import java.util.Objects;

public class RestForgetFollowerAction extends BaseRestHandler {

    public RestForgetFollowerAction(final RestController restController) {
        Objects.requireNonNull(restController);
        restController.registerHandler(RestRequest.Method.POST, "/{index}/_ccr/forget_follower", this);
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
            return Request.fromXContent(parser, leaderIndex);
        }
    }
}
