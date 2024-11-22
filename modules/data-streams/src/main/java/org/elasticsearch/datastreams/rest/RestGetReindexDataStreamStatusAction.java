/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.rest;

import org.elasticsearch.action.datastreams.GetReindexDataStreamStatusAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestGetReindexDataStreamStatusAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "get_reindex_data_stream_status_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_reindex_data_stream_status/{persistent_task_id}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String persistentTaskId = request.param("persistent_task_id");
        GetReindexDataStreamStatusAction.Request getTaskRequest = new GetReindexDataStreamStatusAction.Request(persistentTaskId);
        return channel -> client.execute(GetReindexDataStreamStatusAction.INSTANCE, getTaskRequest, new RestToXContentListener<>(channel));
    }
}
