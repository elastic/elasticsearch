/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.desirednodes.TransportDeleteDesiredNodesAction;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.EmptyResponseListener;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestUtils.getAckTimeout;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;
import static org.elasticsearch.rest.action.EmptyResponseListener.PLAIN_TEXT_EMPTY_RESPONSE_CAPABILITY_NAME;

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
    public Set<String> supportedCapabilities() {
        return Set.of(PLAIN_TEXT_EMPTY_RESPONSE_CAPABILITY_NAME);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final var deleteDesiredNodesRequest = new AcknowledgedRequest.Plain(getMasterNodeTimeout(request), getAckTimeout(request));
        return restChannel -> client.execute(
            TransportDeleteDesiredNodesAction.TYPE,
            deleteDesiredNodesRequest,
            new EmptyResponseListener(restChannel)
        );
    }
}
