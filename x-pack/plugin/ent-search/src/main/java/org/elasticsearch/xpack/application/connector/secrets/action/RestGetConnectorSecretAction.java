/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.secrets.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

@ServerlessScope(Scope.INTERNAL)
public class RestGetConnectorSecretAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "connector_get_secret";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.GET, "/_connector/_secret/{id}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String id = request.param("id");
        return restChannel -> client.execute(
            GetConnectorSecretAction.INSTANCE,
            new GetConnectorSecretRequest(id),
            new RestToXContentListener<>(restChannel)
        );
    }
}
