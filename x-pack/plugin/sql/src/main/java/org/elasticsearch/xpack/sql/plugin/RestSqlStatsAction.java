/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.xpack.sql.action.Protocol;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestSqlStatsAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            Route.builder(GET, Protocol.SQL_STATS_REST_ENDPOINT)
                .replaces(GET, Protocol.SQL_STATS_DEPRECATED_REST_ENDPOINT, RestApiVersion.V_7)
                .build()
        );
    }

    @Override
    public String getName() {
        return "sql_stats";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        SqlStatsRequest request = new SqlStatsRequest();
        return channel -> client.execute(SqlStatsAction.INSTANCE, request, new RestActions.NodesResponseRestListener<>(channel));
    }

}
