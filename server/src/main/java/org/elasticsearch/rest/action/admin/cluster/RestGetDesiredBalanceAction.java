/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.allocation.DesiredBalanceRequest;
import org.elasticsearch.action.admin.cluster.allocation.GetDesiredBalanceAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestChunkedToXContentListener;

import java.io.IOException;
import java.util.List;

public class RestGetDesiredBalanceAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "get_desired_balance";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.GET, "_internal/desired_balance"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        return restChannel -> client.execute(
            GetDesiredBalanceAction.INSTANCE,
            new DesiredBalanceRequest(),
            new RestChunkedToXContentListener<>(restChannel)
        );
    }
}
