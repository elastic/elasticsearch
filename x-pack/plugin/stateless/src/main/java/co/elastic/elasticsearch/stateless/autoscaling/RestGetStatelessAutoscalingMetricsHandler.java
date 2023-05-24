/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.stateless.autoscaling;

import co.elastic.elasticsearch.stateless.autoscaling.action.GetStatelessAutoscalingMetricsAction;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * Rest endpoint for fetching autoscaling metrics. Invoked by CP
 */
@ServerlessScope(Scope.INTERNAL)
public class RestGetStatelessAutoscalingMetricsHandler extends BaseRestHandler {

    @Override
    public String getName() {
        return "get_stateless_autoscaling_metrics";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_internal/stateless/autoscaling"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest restRequest, final NodeClient client) {
        return channel -> client.execute(
            GetStatelessAutoscalingMetricsAction.INSTANCE,
            new GetStatelessAutoscalingMetricsAction.Request(),
            new RestToXContentListener<>(channel)
        );
    }
}
