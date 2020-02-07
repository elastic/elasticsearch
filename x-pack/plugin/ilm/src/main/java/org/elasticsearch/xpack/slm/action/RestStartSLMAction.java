/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.slm.action;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.slm.action.StartSLMAction;

import java.util.List;

import static java.util.Collections.singletonList;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestStartSLMAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return singletonList(new Route(POST, "/_slm/start"));
    }

    @Override
    public String getName() {
        return "slm_start_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        StartSLMAction.Request request = new StartSLMAction.Request();
        request.timeout(restRequest.paramAsTime("timeout", request.timeout()));
        request.masterNodeTimeout(restRequest.paramAsTime("master_timeout", request.masterNodeTimeout()));
        return channel -> client.execute(StartSLMAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
