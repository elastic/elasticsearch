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
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.application.EnterpriseSearch;
import org.elasticsearch.xpack.core.action.util.PageParams;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestListConnectorAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "connector_list_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/" + EnterpriseSearch.CONNECTOR_API_ENDPOINT));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        int from = restRequest.paramAsInt("from", PageParams.DEFAULT_FROM);
        int size = restRequest.paramAsInt("size", PageParams.DEFAULT_SIZE);
        ListConnectorAction.Request request = new ListConnectorAction.Request(new PageParams(from, size));

        return channel -> client.execute(ListConnectorAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
