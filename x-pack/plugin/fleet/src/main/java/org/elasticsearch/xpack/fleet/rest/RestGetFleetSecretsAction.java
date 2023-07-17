/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.fleet.rest;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.fleet.action.GetFleetSecretsAction;

import java.io.IOException;
import java.util.List;

public class RestGetFleetSecretsAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "fleet_secrets_get_pipeline";
    } // TODO: check

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(RestRequest.Method.GET, "/_fleet/_fleet_secrets")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        // TODO: this does not throw IOException
        return restChannel -> client.execute(
            GetFleetSecretsAction.INSTANCE,
            new GetFleetSecretsAction.Request(),
            new RestToXContentListener<>(restChannel)
        );
    }
}
