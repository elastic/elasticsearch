/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.datastreams.rest;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.action.GetDataStreamAction;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestGetDataStreamsAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "get_data_streams_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_data_stream"), new Route(GET, "/_data_stream/{name}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        GetDataStreamAction.Request getDataStreamsRequest = new GetDataStreamAction.Request(
            Strings.splitStringByCommaToArray(request.param("name"))
        );
        getDataStreamsRequest.indicesOptions(IndicesOptions.fromRequest(request, getDataStreamsRequest.indicesOptions()));
        return channel -> client.execute(GetDataStreamAction.INSTANCE, getDataStreamsRequest, new RestToXContentListener<>(channel));
    }

    @Override
    public boolean allowSystemIndexAccessByDefault() {
        return true;
    }
}
