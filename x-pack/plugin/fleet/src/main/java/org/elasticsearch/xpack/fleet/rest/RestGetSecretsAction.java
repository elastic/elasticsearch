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
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.fleet.action.GetSecretAction;
import org.elasticsearch.xpack.fleet.action.GetSecretRequest;

import java.io.IOException;
import java.util.List;

@ServerlessScope(Scope.INTERNAL)
public class RestGetSecretsAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "fleet_get_secret";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.GET, "/_fleet/secret/{id}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String id = request.param("id");
        return restChannel -> client.execute(GetSecretAction.INSTANCE, new GetSecretRequest(id), new RestToXContentListener<>(restChannel));
    }
}
