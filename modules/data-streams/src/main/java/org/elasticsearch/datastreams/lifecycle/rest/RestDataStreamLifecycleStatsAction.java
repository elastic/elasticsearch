/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams.lifecycle.rest;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.datastreams.lifecycle.action.GetDataStreamLifecycleStatsAction;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestChunkedToXContentListener;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

@ServerlessScope(Scope.PUBLIC)
public class RestDataStreamLifecycleStatsAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "data_stream_lifecycle_stats_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_lifecycle/stats"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        String masterNodeTimeout = restRequest.param("master_timeout");
        GetDataStreamLifecycleStatsAction.Request request = new GetDataStreamLifecycleStatsAction.Request();
        if (masterNodeTimeout != null) {
            request.masterNodeTimeout(masterNodeTimeout);
        }
        return channel -> client.execute(GetDataStreamLifecycleStatsAction.INSTANCE, request, new RestChunkedToXContentListener<>(channel));
    }
}
