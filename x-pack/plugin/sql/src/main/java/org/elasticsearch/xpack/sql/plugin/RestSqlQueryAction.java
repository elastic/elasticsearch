/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.sql.action.SqlQueryAction;
import org.elasticsearch.xpack.sql.action.SqlQueryRequest;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.ql.util.LoggingUtils.logOnFailure;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.SQL_QUERY_REST_ENDPOINT;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.URL_PARAM_DELIMITER;

@ServerlessScope(Scope.PUBLIC)
public class RestSqlQueryAction extends BaseRestHandler {
    private static final Logger LOGGER = LogManager.getLogger(RestSqlQueryAction.class);

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, SQL_QUERY_REST_ENDPOINT), new Route(POST, SQL_QUERY_REST_ENDPOINT));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        SqlQueryRequest sqlRequest;
        try (XContentParser parser = request.contentOrSourceParamParser()) {
            sqlRequest = SqlQueryRequest.fromXContent(parser);
        }

        return channel -> {
            RestCancellableNodeClient cancellableClient = new RestCancellableNodeClient(client, request.getHttpChannel());
            cancellableClient.execute(
                SqlQueryAction.INSTANCE,
                sqlRequest,
                new SqlResponseListener(channel, request, sqlRequest).delegateResponse((l, ex) -> {
                    logOnFailure(LOGGER, ex);
                    l.onFailure(ex);
                })
            );
        };
    }

    @Override
    protected Set<String> responseParams() {
        return Collections.singleton(URL_PARAM_DELIMITER);
    }

    @Override
    public String getName() {
        return "sql_query";
    }
}
