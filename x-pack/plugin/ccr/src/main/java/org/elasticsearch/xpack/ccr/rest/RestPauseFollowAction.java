/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.util.List;

import static java.util.Collections.singletonList;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.core.ccr.action.PauseFollowAction.INSTANCE;
import static org.elasticsearch.xpack.core.ccr.action.PauseFollowAction.Request;

public class RestPauseFollowAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return singletonList(new Route(POST, "/{index}/_ccr/pause_follow"));
    }

    @Override
    public String getName() {
        return "ccr_pause_follow_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        Request request = new Request(restRequest.param("index"));
        return channel -> client.execute(INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
