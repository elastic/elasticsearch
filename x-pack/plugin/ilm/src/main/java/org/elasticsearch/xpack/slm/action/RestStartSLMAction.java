/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.slm.action;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.slm.action.StartSLMAction;

public class RestStartSLMAction extends BaseRestHandler {

    public RestStartSLMAction(RestController controller) {
        controller.registerHandler(RestRequest.Method.POST, "/_slm/start", this);
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
