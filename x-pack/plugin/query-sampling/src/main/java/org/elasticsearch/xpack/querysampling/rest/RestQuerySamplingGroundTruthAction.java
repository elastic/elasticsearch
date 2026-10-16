/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.rest;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.querysampling.action.QuerySamplingActions;
import org.elasticsearch.xpack.querysampling.action.QuerySamplingGroundTruthRequest;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

@ServerlessScope(Scope.INTERNAL)
public final class RestQuerySamplingGroundTruthAction extends BaseRestHandler {

    private static final int DEFAULT_MAX = 100;

    @Override
    public String getName() {
        return "query_sampling_ground_truth_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_query_sampling/ground_truth"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        QuerySamplingGroundTruthRequest groundTruthRequest = new QuerySamplingGroundTruthRequest(request.paramAsInt("max", DEFAULT_MAX));
        return channel -> client.execute(QuerySamplingActions.GROUND_TRUTH, groundTruthRequest, new RestToXContentListener<>(channel));
    }
}
