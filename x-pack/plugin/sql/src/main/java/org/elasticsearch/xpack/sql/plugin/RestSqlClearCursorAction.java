/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.plugin;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.sql.action.SqlClearCursorAction;
import org.elasticsearch.xpack.sql.action.SqlClearCursorRequest;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.Protocol;
import org.elasticsearch.xpack.sql.proto.RequestInfo;

import java.io.IOException;
import java.util.Locale;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.sql.proto.RequestInfo.CANVAS;
import static org.elasticsearch.xpack.sql.proto.RequestInfo.CLI;

public class RestSqlClearCursorAction extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(LogManager.getLogger(RestSqlClearCursorAction.class));

    RestSqlClearCursorAction(Settings settings, RestController controller) {
        super(settings);
        // TODO: remove deprecated endpoint in 8.0.0
        controller.registerWithDeprecatedHandler(
                POST, Protocol.CLEAR_CURSOR_REST_ENDPOINT, this,
                POST, Protocol.CLEAR_CURSOR_DEPRECATED_REST_ENDPOINT, deprecationLogger);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        SqlClearCursorRequest sqlRequest;
        try (XContentParser parser = request.contentOrSourceParamParser()) {
            sqlRequest = SqlClearCursorRequest.fromXContent(parser);
        }
        
        // no mode specified, default to "PLAIN"
        if (sqlRequest.requestInfo() == null) {
            sqlRequest.requestInfo(new RequestInfo(Mode.PLAIN));
        }
        if (sqlRequest.requestInfo().clientId() != null) {
            String clientId = sqlRequest.requestInfo().clientId().toLowerCase(Locale.ROOT);
            if (!clientId.equals(CLI) && !clientId.equals(CANVAS)) {
                clientId = null;
            }
            sqlRequest.requestInfo().clientId(clientId);
        }
        return channel -> client.executeLocally(SqlClearCursorAction.INSTANCE, sqlRequest, new RestToXContentListener<>(channel));
    }

    @Override
    public String getName() {
        return "sql_clear_cursor";
    }

}
