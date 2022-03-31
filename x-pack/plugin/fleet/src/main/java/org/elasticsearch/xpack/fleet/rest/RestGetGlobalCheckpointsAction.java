/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.fleet.rest;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.fleet.action.GetGlobalCheckpointsAction;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestGetGlobalCheckpointsAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "fleet_get_global_checkpoints";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/{index}/_fleet/global_checkpoints"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        final String index = request.param("index");
        final boolean waitForAdvance = request.paramAsBoolean("wait_for_advance", false);
        final boolean waitForIndex = request.paramAsBoolean("wait_for_index", false);
        final String[] stringCheckpoints = request.paramAsStringArray("checkpoints", new String[0]);
        final long[] checkpoints = new long[stringCheckpoints.length];
        for (int i = 0; i < stringCheckpoints.length; ++i) {
            checkpoints[i] = Long.parseLong(stringCheckpoints[i]);
        }
        final TimeValue pollTimeout = request.paramAsTime("timeout", TimeValue.timeValueSeconds(30));
        GetGlobalCheckpointsAction.Request getCheckpointsRequest = new GetGlobalCheckpointsAction.Request(
            index,
            waitForAdvance,
            waitForIndex,
            checkpoints,
            pollTimeout
        );
        return channel -> client.execute(GetGlobalCheckpointsAction.INSTANCE, getCheckpointsRequest, new RestToXContentListener<>(channel));
    }
}
