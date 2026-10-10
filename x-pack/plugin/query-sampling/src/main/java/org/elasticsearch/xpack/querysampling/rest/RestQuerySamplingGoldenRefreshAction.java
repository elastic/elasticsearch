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
import org.elasticsearch.xpack.querysampling.action.QuerySamplingGoldenRefreshAction;
import org.elasticsearch.xpack.querysampling.action.QuerySamplingGoldenRefreshRequest;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * Makes a new version of the golden dataset from one, in which the ground truth of the queries whose data changed is computed
 * again. Each of those costs an exact search over the index, so how many queries are looked at per call is bounded by {@code max}.
 */
@ServerlessScope(Scope.INTERNAL)
public final class RestQuerySamplingGoldenRefreshAction extends BaseRestHandler {

    private static final int DEFAULT_MAX = 1000;

    @Override
    public String getName() {
        return "query_sampling_golden_refresh_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_query_sampling/golden/refresh"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        QuerySamplingGoldenRefreshRequest refreshRequest = new QuerySamplingGoldenRefreshRequest(
            request.paramAsLong("version", 0L),
            request.paramAsInt("max", DEFAULT_MAX)
        );
        return channel -> client.execute(QuerySamplingGoldenRefreshAction.INSTANCE, refreshRequest, new RestToXContentListener<>(channel));
    }
}
