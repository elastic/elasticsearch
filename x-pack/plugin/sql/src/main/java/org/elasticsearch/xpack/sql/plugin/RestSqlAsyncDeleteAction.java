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
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.async.DeleteAsyncResultAction;
import org.elasticsearch.xpack.core.async.DeleteAsyncResultRequest;
import org.elasticsearch.xpack.sql.action.SqlManageAsyncRequest;
import org.elasticsearch.xpack.sql.proto.Protocol;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestSqlAsyncDeleteAction extends BaseRestHandler {
    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, Protocol.SQL_ASYNC_DELETE_REST_ENDPOINT));
    }

    @Override
    public String getName() {
        return "sql_post_async_delete";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        SqlManageAsyncRequest asyncRequest;
        try (XContentParser parser = request.contentParser()) {
            asyncRequest = SqlManageAsyncRequest.fromXContent(parser);

        }
        DeleteAsyncResultRequest delete = new DeleteAsyncResultRequest(asyncRequest.id());
        return channel -> client.execute(DeleteAsyncResultAction.INSTANCE, delete, new RestToXContentListener<>(channel));
    }
}
