/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestResponseListener;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.sql.action.SqlClearCursorAction;
import org.elasticsearch.xpack.sql.action.SqlClearCursorRequest;
import org.elasticsearch.xpack.sql.action.SqlClearCursorResponse;
import org.elasticsearch.xpack.sql.proto.Mode;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.CLEAR_CURSOR_REST_ENDPOINT;

@ServerlessScope(Scope.PUBLIC)
public class RestSqlClearCursorAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, CLEAR_CURSOR_REST_ENDPOINT));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        SqlClearCursorRequest sqlRequest;
        try (XContentParser parser = request.contentParser()) {
            sqlRequest = SqlClearCursorRequest.fromXContent(parser);
        }

        return channel -> client.executeLocally(SqlClearCursorAction.INSTANCE, sqlRequest, new RestResponseListener<>(channel) {
            @Override
            public RestResponse buildResponse(SqlClearCursorResponse response) throws Exception {
                Boolean binaryRequest = sqlRequest.binaryCommunication();
                XContentType type = Boolean.TRUE.equals(binaryRequest) || (binaryRequest == null && Mode.isDriver(sqlRequest.mode()))
                    ? XContentType.CBOR
                    : XContentType.JSON;
                XContentBuilder builder = channel.newBuilder(request.getXContentType(), type, false);
                response.toXContent(builder, request);
                return new RestResponse(RestStatus.OK, builder);
            }
        });
    }

    @Override
    public String getName() {
        return "sql_clear_cursor";
    }

}
