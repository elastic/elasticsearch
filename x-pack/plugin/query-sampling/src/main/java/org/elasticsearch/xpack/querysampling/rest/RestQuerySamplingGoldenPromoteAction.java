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
import org.elasticsearch.xpack.querysampling.action.QuerySamplingGoldenPromoteAction;
import org.elasticsearch.xpack.querysampling.action.QuerySamplingGoldenPromoteRequest;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * Promotes stored sampled queries that have their ground truth to a new version of the golden dataset, which is kept
 * for good. How many are promoted per call is bounded by {@code max}.
 */
@ServerlessScope(Scope.INTERNAL)
public final class RestQuerySamplingGoldenPromoteAction extends BaseRestHandler {

    private static final int DEFAULT_MAX = 1000;

    @Override
    public String getName() {
        return "query_sampling_golden_promote_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_query_sampling/golden/promote"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        QuerySamplingGoldenPromoteRequest promoteRequest = new QuerySamplingGoldenPromoteRequest(request.paramAsInt("max", DEFAULT_MAX));
        return channel -> client.execute(QuerySamplingGoldenPromoteAction.INSTANCE, promoteRequest, new RestToXContentListener<>(channel));
    }
}
