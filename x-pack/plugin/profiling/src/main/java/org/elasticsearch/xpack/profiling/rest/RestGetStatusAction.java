/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling.rest;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.profiling.action.GetStatusAction;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;

@ServerlessScope(Scope.PUBLIC)
public class RestGetStatusAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_profiling/status"));
    }

    @Override
    public String getName() {
        return "get_profiling_status_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        GetStatusAction.Request request = new GetStatusAction.Request();
        request.ackTimeout(restRequest.paramAsTime("timeout", request.ackTimeout()));
        request.masterNodeTimeout(getMasterNodeTimeout(restRequest));
        request.waitForResourcesCreated(restRequest.paramAsBoolean("wait_for_resources_created", false));
        return channel -> client.execute(
            GetStatusAction.INSTANCE,
            request,
            new RestToXContentListener<>(channel, GetStatusAction.Response::status)
        );
    }
}
