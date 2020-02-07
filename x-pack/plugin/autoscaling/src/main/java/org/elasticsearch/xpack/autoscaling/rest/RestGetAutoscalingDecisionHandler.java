/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.autoscaling.action.GetAutoscalingDecisionAction;

public class RestGetAutoscalingDecisionHandler extends BaseRestHandler {

    public RestGetAutoscalingDecisionHandler(final RestController controller) {
        controller.registerHandler(RestRequest.Method.GET, "/_autoscaling/decision", this);
    }

    @Override
    public String getName() {
        return "get_autoscaling_decision";
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest restRequest, final NodeClient client) {
        final GetAutoscalingDecisionAction.Request request = new GetAutoscalingDecisionAction.Request();
        return channel -> client.execute(GetAutoscalingDecisionAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }

}
