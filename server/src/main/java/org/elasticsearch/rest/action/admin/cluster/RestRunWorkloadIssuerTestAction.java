/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.workload.WorkloadIssuerTestAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

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
