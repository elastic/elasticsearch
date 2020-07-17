/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ilm.action;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ilm.StopILMRequest;
import org.elasticsearch.xpack.core.ilm.action.StopILMAction;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestStopAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_ilm/stop"));
    }

    @Override
    public String getName() {
        return "ilm_stop_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        StopILMRequest request = new StopILMRequest();
        request.timeout(restRequest.paramAsTime("timeout", request.timeout()));
        request.masterNodeTimeout(restRequest.paramAsTime("master_timeout", request.masterNodeTimeout()));
        return channel -> client.execute(StopILMAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
