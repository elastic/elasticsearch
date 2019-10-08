/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.sql.action.SqlTranslateAction;
import org.elasticsearch.xpack.sql.action.SqlTranslateRequest;
import org.elasticsearch.xpack.sql.proto.Protocol;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * REST action for translating SQL queries into ES requests
 */
public class RestSqlTranslateAction extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(LogManager.getLogger(RestSqlTranslateAction.class));

    public RestSqlTranslateAction(RestController controller) {
        // TODO: remove deprecated endpoint in 8.0.0
        controller.registerWithDeprecatedHandler(
                GET, Protocol.SQL_TRANSLATE_REST_ENDPOINT, this,
                GET, Protocol.SQL_TRANSLATE_DEPRECATED_REST_ENDPOINT, deprecationLogger);
        // TODO: remove deprecated endpoint in 8.0.0
        controller.registerWithDeprecatedHandler(
                POST, Protocol.SQL_TRANSLATE_REST_ENDPOINT, this,
                POST, Protocol.SQL_TRANSLATE_DEPRECATED_REST_ENDPOINT, deprecationLogger);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client)
            throws IOException {
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

