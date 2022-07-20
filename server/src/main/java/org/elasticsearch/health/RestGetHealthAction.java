/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestGetHealthAction extends BaseRestHandler {

    private static final String EXPLAIN_PARAM = "explain";

    @Override
    public String getName() {
        // TODO: Existing - "cluster_health_action", "cat_health_action"
        return "health_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_internal/_health"),
            new Route(GET, "/_internal/_health/{component}"),
            new Route(GET, "/_internal/_health/{component}/{indicator}")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String componentName = request.param("component");
        String indicatorName = request.param("indicator");
        boolean explain = request.paramAsBoolean(EXPLAIN_PARAM, true);
        GetHealthAction.Request getHealthRequest = componentName == null
            ? new GetHealthAction.Request(explain)
            : new GetHealthAction.Request(componentName, indicatorName, explain);
        return channel -> client.execute(GetHealthAction.INSTANCE, getHealthRequest, new RestToXContentListener<>(channel));
    }
}
