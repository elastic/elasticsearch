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
import org.elasticsearch.xpack.application.connector.Connector;
import org.elasticsearch.xpack.core.action.util.PageParams;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

@ServerlessScope(Scope.PUBLIC)
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
        List<String> indexNames = List.of(restRequest.paramAsStringArray(Connector.INDEX_NAME_FIELD.getPreferredName(), new String[0]));
        List<String> connectorNames = List.of(restRequest.paramAsStringArray("connector_name", new String[0]));
        List<String> serviceTypes = List.of(restRequest.paramAsStringArray("service_type", new String[0]));
        String searchQuery = restRequest.param("query");
        Boolean includeDeleted = restRequest.paramAsBoolean("include_deleted", false);

        ListConnectorAction.Request request = new ListConnectorAction.Request(
            new PageParams(from, size),
            indexNames,
            connectorNames,
            serviceTypes,
            searchQuery,
            includeDeleted
        );

        return channel -> client.execute(ListConnectorAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
