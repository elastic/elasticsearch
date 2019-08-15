/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.plugin;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.xpack.sql.proto.Protocol;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestSqlStatsAction extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(LogManager.getLogger(RestSqlStatsAction.class));

    protected RestSqlStatsAction(RestController controller) {
        // TODO: remove deprecated endpoint in 8.0.0
        controller.registerWithDeprecatedHandler(
                GET, Protocol.SQL_STATS_REST_ENDPOINT, this,
                GET, Protocol.SQL_STATS_DEPRECATED_REST_ENDPOINT, deprecationLogger);
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
