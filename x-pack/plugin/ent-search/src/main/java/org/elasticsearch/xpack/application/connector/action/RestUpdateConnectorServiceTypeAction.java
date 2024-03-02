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
import org.elasticsearch.xpack.application.EnterpriseSearch;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.PUT;

@ServerlessScope(Scope.PUBLIC)
public class RestUpdateConnectorServiceTypeAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "connector_update_service_type_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, "/" + EnterpriseSearch.CONNECTOR_API_ENDPOINT + "/{connector_id}/_service_type"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        UpdateConnectorServiceTypeAction.Request request = UpdateConnectorServiceTypeAction.Request.fromXContentBytes(
            restRequest.param("connector_id"),
            restRequest.content(),
            restRequest.getXContentType()
        );
        return channel -> client.execute(
            UpdateConnectorServiceTypeAction.INSTANCE,
            request,
            new RestToXContentListener<>(channel, ConnectorUpdateActionResponse::status)
        );
    }
}
