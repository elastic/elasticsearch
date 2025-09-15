/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.migrate.rest;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.migrate.action.CancelReindexDataStreamAction;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestCancelReindexDataStreamAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "cancel_reindex_data_stream_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_migration/reindex/{index}/_cancel"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String index = request.param("index");
        CancelReindexDataStreamAction.Request cancelTaskRequest = new CancelReindexDataStreamAction.Request(index);
        return channel -> client.execute(CancelReindexDataStreamAction.INSTANCE, cancelTaskRequest, new RestToXContentListener<>(channel));
    }
}
