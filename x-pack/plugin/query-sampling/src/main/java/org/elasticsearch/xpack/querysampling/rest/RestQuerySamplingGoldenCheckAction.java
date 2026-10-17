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
import org.elasticsearch.xpack.querysampling.action.QuerySamplingGoldenCheckRequest;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

@ServerlessScope(Scope.INTERNAL)
public final class RestQuerySamplingGoldenCheckAction extends BaseRestHandler {

    private static final int DEFAULT_MAX = 1000;

    @Override
    public String getName() {
        return "query_sampling_golden_check_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_query_sampling/golden/check"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        QuerySamplingGoldenCheckRequest checkRequest = new QuerySamplingGoldenCheckRequest(
            request.paramAsLong("version", 0L),
            request.paramAsInt("max", DEFAULT_MAX)
        );
        return channel -> client.execute(QuerySamplingActions.GOLDEN_CHECK, checkRequest, new RestToXContentListener<>(channel));
    }
}
