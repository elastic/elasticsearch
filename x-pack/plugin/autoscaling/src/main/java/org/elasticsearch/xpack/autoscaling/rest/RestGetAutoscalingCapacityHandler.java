/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.autoscaling.action.GetAutoscalingCapacityAction;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestGetAutoscalingCapacityHandler extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_autoscaling/capacity"));
    }

    @Override
    public String getName() {
        return "get_autoscaling_capacity";
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest restRequest, final NodeClient client) {
        final GetAutoscalingCapacityAction.Request request = new GetAutoscalingCapacityAction.Request();
        return channel -> client.execute(GetAutoscalingCapacityAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }

}
