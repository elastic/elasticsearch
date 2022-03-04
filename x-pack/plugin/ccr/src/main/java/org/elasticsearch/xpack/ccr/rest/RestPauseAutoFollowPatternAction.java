/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ccr.rest;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ccr.action.ActivateAutoFollowPatternAction.Request;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.core.ccr.action.ActivateAutoFollowPatternAction.INSTANCE;

public class RestPauseAutoFollowPatternAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_ccr/auto_follow/{name}/pause"));
    }

    @Override
    public String getName() {
        return "ccr_pause_auto_follow_pattern_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest restRequest, final NodeClient client) {
        Request request = new Request(restRequest.param("name"), false);
        return channel -> client.execute(INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
