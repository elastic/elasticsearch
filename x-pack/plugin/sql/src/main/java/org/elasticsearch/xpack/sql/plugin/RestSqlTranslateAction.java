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
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.sql.action.Protocol;
import org.elasticsearch.xpack.sql.action.SqlTranslateAction;
import org.elasticsearch.xpack.sql.action.SqlTranslateRequest;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * REST action for translating SQL queries into ES requests
 */
@ServerlessScope(Scope.PUBLIC)
public class RestSqlTranslateAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            Route.builder(GET, Protocol.SQL_TRANSLATE_REST_ENDPOINT)
                .replaces(GET, Protocol.SQL_TRANSLATE_DEPRECATED_REST_ENDPOINT, RestApiVersion.V_7)
                .build(),
            Route.builder(POST, Protocol.SQL_TRANSLATE_REST_ENDPOINT)
                .replaces(POST, Protocol.SQL_TRANSLATE_DEPRECATED_REST_ENDPOINT, RestApiVersion.V_7)
                .build()
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        SqlTranslateRequest sqlRequest;
        try (XContentParser parser = request.contentOrSourceParamParser()) {
            sqlRequest = SqlTranslateRequest.fromXContent(parser);
        }

        return channel -> client.executeLocally(SqlTranslateAction.INSTANCE, sqlRequest, new RestToXContentListener<>(channel));
    }

    @Override
    public String getName() {
        return "xpack_sql_translate_action";
    }
}
