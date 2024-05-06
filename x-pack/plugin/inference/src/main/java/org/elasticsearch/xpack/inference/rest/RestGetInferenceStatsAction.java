/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rest;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.inference.action.GetInferenceStatsAction;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.xpack.inference.rest.Paths.INFERENCE_STATS_PATH;

@ServerlessScope(Scope.INTERNAL)
public class RestGetInferenceStatsAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "get_inference_stats_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, INFERENCE_STATS_PATH));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        return channel -> client.execute(
            GetInferenceStatsAction.INSTANCE,
            new GetInferenceStatsAction.Request(),
            new RestToXContentListener<>(channel)
        );
    }
}
