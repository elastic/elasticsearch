/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.rest;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.xpack.querysampling.action.QuerySamplingStatsAction;
import org.elasticsearch.xpack.querysampling.action.QuerySamplingStatsRequest;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

@ServerlessScope(Scope.INTERNAL)
public final class RestQuerySamplingStatsAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "query_sampling_stats_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_query_sampling/stats"), new Route(GET, "/_query_sampling/stats/{node_id}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        String[] nodesIds = Strings.splitStringByCommaToArray(request.param("node_id"));
        QuerySamplingStatsRequest statsRequest = new QuerySamplingStatsRequest(nodesIds);
        return channel -> client.execute(
            QuerySamplingStatsAction.INSTANCE,
            statsRequest,
            new RestActions.NodesResponseRestListener<>(channel)
        );
    }
}
