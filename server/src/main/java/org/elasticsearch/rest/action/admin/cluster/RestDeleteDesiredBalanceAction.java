/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.allocation.DesiredBalanceRequest;
import org.elasticsearch.action.admin.cluster.allocation.TransportDeleteDesiredBalanceAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.EmptyResponseListener;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.action.EmptyResponseListener.PLAIN_TEXT_EMPTY_RESPONSE_CAPABILITY_NAME;

@ServerlessScope(Scope.INTERNAL)
public class RestDeleteDesiredBalanceAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "delete_desired_balance";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.DELETE, "_internal/desired_balance"));
    }

    @Override
    public Set<String> supportedCapabilities() {
        return Set.of(PLAIN_TEXT_EMPTY_RESPONSE_CAPABILITY_NAME);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final var req = new DesiredBalanceRequest(RestUtils.getMasterNodeTimeout(request));
        return channel -> client.execute(TransportDeleteDesiredBalanceAction.TYPE, req, new EmptyResponseListener(channel));
    }
}
