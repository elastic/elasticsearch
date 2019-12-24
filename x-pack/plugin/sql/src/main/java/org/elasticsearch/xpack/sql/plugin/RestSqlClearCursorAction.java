/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.client.node.NodeClient;
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

    public RestSqlClearCursorAction(RestController controller) {
        controller.registerHandler(POST, Protocol.CLEAR_CURSOR_REST_ENDPOINT, this);
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
