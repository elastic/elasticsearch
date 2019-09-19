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
import org.elasticsearch.xpack.sql.action.SqlClearCursorAction;
import org.elasticsearch.xpack.sql.action.SqlClearCursorRequest;
import org.elasticsearch.xpack.sql.proto.Protocol;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestSqlClearCursorAction extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(LogManager.getLogger(RestSqlClearCursorAction.class));

    public RestSqlClearCursorAction(RestController controller) {
        // TODO: remove deprecated endpoint in 8.0.0
        controller.registerWithDeprecatedHandler(
                POST, Protocol.CLEAR_CURSOR_REST_ENDPOINT, this,
                POST, Protocol.CLEAR_CURSOR_DEPRECATED_REST_ENDPOINT, deprecationLogger);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client)
            throws IOException {
        SqlClearCursorRequest sqlRequest;
        try (XContentParser parser = request.contentParser()) {
            sqlRequest = SqlClearCursorRequest.fromXContent(parser);
        }

        return channel -> client.executeLocally(SqlClearCursorAction.INSTANCE, sqlRequest, new RestToXContentListener<>(channel));
    }

    @Override
    public String getName() {
        return "sql_clear_cursor";
    }

}
