/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.derivedmetrics.rest;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.datastreams.derivedmetrics.action.GetDerivedMetricsStatsAction;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * {@code GET /_derived_metrics/stats}, sitting beside {@code GET /_lifecycle/stats} rather than under {@code /_data_stream/{name}}.
 *
 * <p>The two are the same kind of thing — a feature of data streams reporting on itself — and the answer is not scoped to one data stream,
 * so hanging it off a name-bearing path would have meant either inventing a wildcard the action does not honour, or a route that reads like
 * a per-stream API while returning the whole node's view.
 */
@ServerlessScope(Scope.PUBLIC)
public class RestDerivedMetricsStatsAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "derived_metrics_stats_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_derived_metrics/stats"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        GetDerivedMetricsStatsAction.Request request = new GetDerivedMetricsStatsAction.Request();
        return channel -> client.execute(GetDerivedMetricsStatsAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
