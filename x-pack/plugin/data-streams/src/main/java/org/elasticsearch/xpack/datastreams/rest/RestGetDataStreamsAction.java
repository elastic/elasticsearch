/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.datastreams.rest;

import org.elasticsearch.xpack.core.action.GetDataStreamAction;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

public class RestGetDataStreamsAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "get_data_streams_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.GET, "/_data_stream"), new Route(RestRequest.Method.GET, "/_data_stream/{name}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        GetDataStreamAction.Request getDataStreamsRequest = new GetDataStreamAction.Request(
            Strings.splitStringByCommaToArray(request.param("name"))
        );
        return channel -> client.execute(GetDataStreamAction.INSTANCE, getDataStreamsRequest, new RestToXContentListener<>(channel));
    }
}
