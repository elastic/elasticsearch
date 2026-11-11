/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestActions;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * Live ES|QL datasource admission gauges (used, limit, untracked, waiters, permits, rescues).
 * Core {@code GET /_nodes/stats} is not pluggable for x-pack plugins. Soak must sample
 * {@code GET /_esql/stats} <em>during</em> load; after-query occupancy is always idle.
 */
@ServerlessScope(Scope.INTERNAL)
public class RestEsqlStatsAction extends BaseRestHandler {

    public static final String STATS_REST_ENDPOINT = "/_esql/stats";

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, STATS_REST_ENDPOINT));
    }

    @Override
    public String getName() {
        return "esql_stats";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        EsqlStatsRequest request = new EsqlStatsRequest();
        request.includeStats(true);
        return channel -> client.execute(EsqlStatsAction.INSTANCE, request, new RestActions.NodesResponseRestListener<>(channel));
    }
}
