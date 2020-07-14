/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.datastreams.rest;

import org.elasticsearch.xpack.core.action.DeleteDataStreamAction;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

public class RestDeleteDataStreamAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "delete_data_stream_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.DELETE, "/_data_stream/{name}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        DeleteDataStreamAction.Request deleteDataStreamRequest = new DeleteDataStreamAction.Request(
            Strings.splitStringByCommaToArray(request.param("name"))
        );
        return channel -> client.execute(DeleteDataStreamAction.INSTANCE, deleteDataStreamRequest, new RestToXContentListener<>(channel));
    }
}
