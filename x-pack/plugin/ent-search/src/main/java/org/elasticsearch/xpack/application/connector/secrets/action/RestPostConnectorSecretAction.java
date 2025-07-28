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
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

@ServerlessScope(Scope.INTERNAL)
public class RestPostConnectorSecretAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "connector_post_secret";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.POST, "/_connector/_secret"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        try (XContentParser parser = request.contentParser()) {
            PostConnectorSecretRequest postSecretRequest = PostConnectorSecretRequest.fromXContent(parser);
            return restChannel -> client.execute(
                PostConnectorSecretAction.INSTANCE,
                postSecretRequest,
                new RestToXContentListener<>(restChannel)
            );
        }
    }
}
