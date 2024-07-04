/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.datastreams.rest;

import org.elasticsearch.action.datastreams.DataStreamsStatsAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

@ServerlessScope(Scope.PUBLIC)
public class RestDataStreamsStatsAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "data_stream_stats_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_data_stream/_stats"), new Route(GET, "/_data_stream/{name}/_stats"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        DataStreamsStatsAction.Request dataStreamsStatsRequest = new DataStreamsStatsAction.Request();
        dataStreamsStatsRequest.indices(Strings.splitStringByCommaToArray(request.param("name")));
        dataStreamsStatsRequest.indicesOptions(IndicesOptions.fromRequest(request, dataStreamsStatsRequest.indicesOptions()));
        return channel -> client.execute(DataStreamsStatsAction.INSTANCE, dataStreamsStatsRequest, new RestToXContentListener<>(channel));
    }

    @Override
    public boolean allowSystemIndexAccessByDefault() {
        return true;
    }
}
