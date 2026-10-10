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
import org.elasticsearch.xpack.querysampling.action.QuerySamplingRecallAction;
import org.elasticsearch.xpack.querysampling.action.QuerySamplingRecallRequest;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * Estimates the recall of the search from the stored sampled queries whose ground truth has been computed.
 */
@ServerlessScope(Scope.INTERNAL)
public final class RestQuerySamplingRecallAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "query_sampling_recall_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_query_sampling/recall"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        QuerySamplingRecallRequest recallRequest = new QuerySamplingRecallRequest(
            request.paramAsInt("max", QuerySamplingRecallRequest.MAX_SAMPLES),
            request.paramAsBoolean("include_samples", false)
        );
        return channel -> client.execute(QuerySamplingRecallAction.INSTANCE, recallRequest, new RestToXContentListener<>(channel));
    }
}
