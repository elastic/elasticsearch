/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.datastreams.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.action.DataStreamsStatsAction;

import java.io.IOException;
import java.util.List;

public class RestDataStreamsStatsAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "data_stream_stats_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(RestRequest.Method.GET, "/_data_stream/_stats"),
            new Route(RestRequest.Method.GET, "/_data_stream/{name}/_stats")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        DataStreamsStatsAction.Request dataStreamsStatsRequest = new DataStreamsStatsAction.Request();
        dataStreamsStatsRequest.indices(Strings.splitStringByCommaToArray(request.param("name")));
        return channel -> client.execute(DataStreamsStatsAction.INSTANCE, dataStreamsStatsRequest, new RestToXContentListener<>(channel));
    }
}
