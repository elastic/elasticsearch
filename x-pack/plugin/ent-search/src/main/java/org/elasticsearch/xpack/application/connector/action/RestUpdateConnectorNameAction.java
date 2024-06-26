/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.application.EnterpriseSearch;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.PUT;

@ServerlessScope(Scope.PUBLIC)
public class RestUpdateConnectorNameAction extends BaseRestHandler {

    private static final String CONNECTOR_ID_PARAM = "connector_id";

    @Override
    public String getName() {
        return "connector_update_name_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, "/" + EnterpriseSearch.CONNECTOR_API_ENDPOINT + "/{" + CONNECTOR_ID_PARAM + "}/_name"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        try (XContentParser parser = restRequest.contentParser()) {
            UpdateConnectorNameAction.Request request = UpdateConnectorNameAction.Request.fromXContent(
                parser,
                restRequest.param(CONNECTOR_ID_PARAM)
            );
            return channel -> client.execute(
                UpdateConnectorNameAction.INSTANCE,
                request,
                new RestToXContentListener<>(channel, ConnectorUpdateActionResponse::status)
            );
        }
    }
}
