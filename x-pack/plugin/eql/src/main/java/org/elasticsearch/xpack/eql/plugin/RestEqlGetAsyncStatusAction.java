/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.eql.plugin;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestStatusToXContentListener;
import org.elasticsearch.xpack.core.async.GetAsyncStatusRequest;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestEqlGetAsyncStatusAction extends BaseRestHandler {
    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_eql/search/status/{id}"));
    }

    @Override
    public String getName() {
        return "eql_get_async_status";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        GetAsyncStatusRequest statusRequest = new GetAsyncStatusRequest(request.param("id"));
        return channel -> client.execute(EqlAsyncGetStatusAction.INSTANCE, statusRequest, new RestStatusToXContentListener<>(channel));
    }
}
