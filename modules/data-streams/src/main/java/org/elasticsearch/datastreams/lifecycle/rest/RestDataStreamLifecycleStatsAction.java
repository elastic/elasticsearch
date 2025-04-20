/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.lifecycle.rest;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.datastreams.lifecycle.action.GetDataStreamLifecycleStatsAction;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestRefCountedChunkedToXContentListener;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;

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
        final var request = new GetDataStreamLifecycleStatsAction.Request(getMasterNodeTimeout(restRequest));
        return channel -> client.execute(
            GetDataStreamLifecycleStatsAction.INSTANCE,
            request,
            new RestRefCountedChunkedToXContentListener<>(channel)
        );
    }
}
