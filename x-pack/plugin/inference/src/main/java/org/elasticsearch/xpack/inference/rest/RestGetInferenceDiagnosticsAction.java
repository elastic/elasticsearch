/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rest;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.inference.action.GetInferenceDiagnosticsAction;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.xpack.inference.rest.Paths.INFERENCE_DIAGNOSTICS_PATH;

@ServerlessScope(Scope.INTERNAL)
public class RestGetInferenceDiagnosticsAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "get_inference_diagnostics_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, INFERENCE_DIAGNOSTICS_PATH));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        return channel -> client.execute(
            GetInferenceDiagnosticsAction.INSTANCE,
            new GetInferenceDiagnosticsAction.Request(),
            new RestToXContentListener<>(channel)
        );
    }
}
