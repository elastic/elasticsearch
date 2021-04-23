/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestStatusToXContentListener;
import org.elasticsearch.xpack.core.async.GetAsyncStatusRequest;
import org.elasticsearch.xpack.sql.action.SqlManageAsyncRequest;
import org.elasticsearch.xpack.sql.proto.Protocol;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestSqlAsyncStatusAction extends BaseRestHandler {
    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, Protocol.SQL_ASYNC_STATUS_REST_ENDPOINT));
    }

    @Override
    public String getName() {
        return "sql_get_async_status";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        SqlManageAsyncRequest asyncRequest;
        try (XContentParser parser = request.contentParser()) {
            asyncRequest = SqlManageAsyncRequest.fromXContent(parser);

        }
        GetAsyncStatusRequest statusRequest = new GetAsyncStatusRequest(asyncRequest.id());
        return channel -> client.execute(SqlAsyncStatusAction.INSTANCE, statusRequest, new RestStatusToXContentListener<>(channel));
    }
}
