/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ccr.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ccr.action.DeleteAutoFollowPatternAction.Request;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;
import static org.elasticsearch.xpack.core.ccr.action.DeleteAutoFollowPatternAction.INSTANCE;

public class RestDeleteAutoFollowPatternAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(DELETE, "/_ccr/auto_follow/{name}"));
    }

    @Override
    public String getName() {
        return "ccr_delete_auto_follow_pattern_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        Request request = new Request(restRequest.param("name"));
        return channel -> client.execute(INSTANCE, request, new RestToXContentListener<>(channel));
    }

}
