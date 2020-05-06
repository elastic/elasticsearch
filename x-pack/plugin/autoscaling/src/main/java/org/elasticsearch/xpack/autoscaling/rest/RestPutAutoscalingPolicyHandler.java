/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.autoscaling.action.PutAutoscalingPolicyAction;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class RestPutAutoscalingPolicyHandler extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, "/_autoscaling/policy/{name}"));
    }

    @Override
    public String getName() {
        return "put_autoscaling_policy";
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest restRequest, final NodeClient client) throws IOException {
        final String name = restRequest.param("name");
        final PutAutoscalingPolicyAction.Request request;
        try (XContentParser parser = restRequest.contentParser()) {
            request = PutAutoscalingPolicyAction.Request.parse(parser, name);
        }
        return channel -> client.execute(PutAutoscalingPolicyAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }

}
