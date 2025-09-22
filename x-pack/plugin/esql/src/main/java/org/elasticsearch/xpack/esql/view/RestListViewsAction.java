/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

@ServerlessScope(Scope.PUBLIC)
public class RestListViewsAction extends BaseRestHandler {
    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_query/views"));
    }

    @Override
    public String getName() {
        return "esql_list_views";
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        ListViewsAction.Request req = new ListViewsAction.Request();
        return channel -> client.execute(TransportListViewsAction.TYPE, req, new RestToXContentListener<>(channel));
    }
}
