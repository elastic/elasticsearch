/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.search.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.application.EnterpriseSearch;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

@ServerlessScope(Scope.PUBLIC)
public class RestDeleteSearchApplicationAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "search_application_delete_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(DELETE, "/" + EnterpriseSearch.SEARCH_APPLICATION_API_ENDPOINT + "/{name}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        DeleteSearchApplicationAction.Request request = new DeleteSearchApplicationAction.Request(restRequest.param("name"));
        return channel -> client.execute(DeleteSearchApplicationAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
