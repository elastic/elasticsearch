/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.slm.action.StopSLMAction;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestUtils.REST_MASTER_TIMEOUT_PARAM;
import static org.elasticsearch.rest.RestUtils.REST_TIMEOUT_PARAM;
import static org.elasticsearch.rest.RestUtils.getAckTimeout;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;

@ServerlessScope(Scope.INTERNAL)
public class RestStopSLMAction extends BaseRestHandler {

    private static final Set<String> SUPPORTED_QUERY_PARAMETERS = Set.of(REST_MASTER_TIMEOUT_PARAM, REST_TIMEOUT_PARAM);

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_slm/stop"));
    }

    @Override
    public String getName() {
        return "slm_stop_action";
    }

    @Override
    public Set<String> supportedQueryParameters() {
        return SUPPORTED_QUERY_PARAMETERS;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        final var request = new StopSLMAction.Request(getMasterNodeTimeout(restRequest), getAckTimeout(restRequest));
        return channel -> client.execute(StopSLMAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
