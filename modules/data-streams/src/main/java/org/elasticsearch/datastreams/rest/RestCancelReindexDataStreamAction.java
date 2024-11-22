/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.rest;

import org.elasticsearch.action.datastreams.CancelReindexDataStreamAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

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
        return List.of(new Route(POST, "/_reindex_data_stream/{persistent_task_id}/_cancel"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String persistentTaskId = request.param("persistent_task_id");
        CancelReindexDataStreamAction.Request cancelTaskRequest = new CancelReindexDataStreamAction.Request(persistentTaskId);
        return channel -> client.execute(CancelReindexDataStreamAction.INSTANCE, cancelTaskRequest, new RestToXContentListener<>(channel));
    }
}
