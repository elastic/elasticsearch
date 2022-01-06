/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.license;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestGetTrialStatus extends BaseRestHandler {

    RestGetTrialStatus() {}

    @Override
    public List<Route> routes() {
        return List.of(
            Route.builder(GET, "/_license/trial_status").replaces(GET, "/_xpack/license/trial_status", RestApiVersion.V_7).build()
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        return channel -> new GetTrialStatusRequestBuilder(client).execute(new RestToXContentListener<>(channel));
    }

    @Override
    public String getName() {
        return "get_trial_status";
    }

}
