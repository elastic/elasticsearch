/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.security.action.workload.WorkloadIssuerTestAction;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestRunWorkloadIssuerTestAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_run_workload_issuer_test"));
    }

    @Override
    public String getName() {
        return "run_workload_issuer_test_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        return channel -> client.execute(
            WorkloadIssuerTestAction.INSTANCE,
            new WorkloadIssuerTestAction.Request(),
            new RestToXContentListener<>(channel)
        );
    }
}
