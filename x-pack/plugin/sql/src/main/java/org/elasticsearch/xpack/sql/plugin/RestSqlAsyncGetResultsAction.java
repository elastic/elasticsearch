/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.xpack.core.async.GetAsyncResultRequest;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.xpack.sql.proto.Protocol.ID_NAME;
import static org.elasticsearch.xpack.sql.proto.Protocol.KEEP_ALIVE_NAME;
import static org.elasticsearch.xpack.sql.proto.Protocol.SQL_ASYNC_REST_ENDPOINT;
import static org.elasticsearch.xpack.sql.proto.Protocol.URL_PARAM_DELIMITER;
import static org.elasticsearch.xpack.sql.proto.Protocol.WAIT_FOR_COMPLETION_TIMEOUT_NAME;

public class RestSqlAsyncGetResultsAction extends BaseRestHandler {
    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, SQL_ASYNC_REST_ENDPOINT + "{" + ID_NAME + "}"));
    }

    @Override
    public String getName() {
        return "sql_get_async_result";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        GetAsyncResultRequest get = new GetAsyncResultRequest(request.param(ID_NAME));
        if (request.hasParam(WAIT_FOR_COMPLETION_TIMEOUT_NAME)) {
            get.setWaitForCompletionTimeout(request.paramAsTime(WAIT_FOR_COMPLETION_TIMEOUT_NAME, get.getWaitForCompletionTimeout()));
        }
        if (request.hasParam(KEEP_ALIVE_NAME)) {
            get.setKeepAlive(request.paramAsTime(KEEP_ALIVE_NAME, get.getKeepAlive()));
        }
        return channel -> client.execute(SqlAsyncGetResultsAction.INSTANCE, get, new SqlResponseListener(channel, request));
    }

    @Override
    protected Set<String> responseParams() {
        return Collections.singleton(URL_PARAM_DELIMITER);
    }

}
