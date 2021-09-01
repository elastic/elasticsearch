/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rollup.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.rollup.RollupField;
import org.elasticsearch.xpack.core.rollup.action.StartRollupJobAction;

import java.util.List;

import static java.util.Collections.singletonList;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestStartRollupJobAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return singletonList(
            Route.builder(POST, "/_rollup/job/{id}/_start").replaces(POST, "/_xpack/rollup/job/{id}/_start", RestApiVersion.V_7).build()
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        String id = restRequest.param(RollupField.ID.getPreferredName());
        StartRollupJobAction.Request request = new StartRollupJobAction.Request(id);

        return channel -> client.execute(StartRollupJobAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }

    @Override
    public String getName() {
        return "start_rollup_job";
    }

}
