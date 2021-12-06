/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rollup.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.rollup.RollupField;
import org.elasticsearch.xpack.core.rollup.action.StopRollupJobAction;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestStopRollupJobAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            Route.builder(POST, "/_rollup/job/{id}/_stop").replaces(POST, "/_xpack/rollup/job/{id}/_stop", RestApiVersion.V_7).build()
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        String id = restRequest.param(RollupField.ID.getPreferredName());
        TimeValue timeout = restRequest.paramAsTime(StopRollupJobAction.TIMEOUT.getPreferredName(), StopRollupJobAction.DEFAULT_TIMEOUT);
        boolean waitForCompletion = restRequest.paramAsBoolean(StopRollupJobAction.WAIT_FOR_COMPLETION.getPreferredName(), false);
        StopRollupJobAction.Request request = new StopRollupJobAction.Request(id, waitForCompletion, timeout);

        return channel -> client.execute(StopRollupJobAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }

    @Override
    public String getName() {
        return "stop_rollup_job";
    }

}
