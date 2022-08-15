/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.desirednodes.DeleteDesiredNodesAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

public class RestDeleteDesiredNodesAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "delete_desired_nodes";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.DELETE, "_internal/desired_nodes"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final DeleteDesiredNodesAction.Request deleteDesiredNodesRequest = new DeleteDesiredNodesAction.Request();
        deleteDesiredNodesRequest.masterNodeTimeout(request.paramAsTime("master_timeout", deleteDesiredNodesRequest.masterNodeTimeout()));
        return restChannel -> client.execute(
            DeleteDesiredNodesAction.INSTANCE,
            deleteDesiredNodesRequest,
            new RestToXContentListener<>(restChannel)
        );
    }
}
