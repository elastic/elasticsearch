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
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.application.EnterpriseSearch;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestPostConnectorAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "connector_post_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/" + EnterpriseSearch.CONNECTOR_API_ENDPOINT));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        PostConnectorAction.Request request = PostConnectorAction.Request.fromXContentBytes(
            restRequest.content(),
            restRequest.getXContentType()
        );
        return channel -> client.execute(
            PostConnectorAction.INSTANCE,
            request,
            new RestToXContentListener<>(channel, r -> RestStatus.CREATED, r -> null)
        );
    }
}
