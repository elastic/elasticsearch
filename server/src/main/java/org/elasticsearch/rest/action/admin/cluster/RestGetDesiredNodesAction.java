/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.desirednodes.GetDesiredNodesAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

public class RestGetDesiredNodesAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "get_desired_nodes";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.GET, "_internal/desired_nodes/_latest"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final GetDesiredNodesAction.Request getDesiredNodesRequest = new GetDesiredNodesAction.Request();
        getDesiredNodesRequest.masterNodeTimeout(request.paramAsTime("master_timeout", getDesiredNodesRequest.masterNodeTimeout()));
        return restChannel -> client.execute(
            GetDesiredNodesAction.INSTANCE,
            getDesiredNodesRequest,
            new RestToXContentListener<>(restChannel)
        );
    }
}
