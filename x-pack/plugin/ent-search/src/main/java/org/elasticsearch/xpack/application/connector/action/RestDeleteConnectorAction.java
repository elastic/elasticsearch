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

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

@ServerlessScope(Scope.PUBLIC)
public class RestDeleteConnectorAction extends BaseRestHandler {

    private static final String CONNECTOR_ID_PARAM = "connector_id";

    @Override
    public String getName() {
        return "connector_delete_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(DELETE, "/" + EnterpriseSearch.CONNECTOR_API_ENDPOINT + "/{" + CONNECTOR_ID_PARAM + "}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {

        String connectorId = restRequest.param(CONNECTOR_ID_PARAM);
        boolean shouldDeleteSyncJobs = restRequest.paramAsBoolean("delete_sync_jobs", false);
        boolean hardDelete = restRequest.paramAsBoolean("hard", false);

        DeleteConnectorAction.Request request = new DeleteConnectorAction.Request(connectorId, hardDelete, shouldDeleteSyncJobs);
        return channel -> client.execute(DeleteConnectorAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
