/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rollup.rest;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.core.rollup.action.GetRollupJobsAction;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestGetRollupJobsAction extends BaseRestHandler {

    public static final ParseField ID = new ParseField("id");

    @Override
    public List<Route> routes() {
        return List.of(Route.builder(GET, "/_rollup/job/{id}").replaces(GET, "/_xpack/rollup/job/{id}/", RestApiVersion.V_7).build());
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        String id = restRequest.param(ID.getPreferredName());
        GetRollupJobsAction.Request request = new GetRollupJobsAction.Request(id);

        return channel -> client.execute(GetRollupJobsAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }

    @Override
    public String getName() {
        return "get_rollup_job";
    }

}
