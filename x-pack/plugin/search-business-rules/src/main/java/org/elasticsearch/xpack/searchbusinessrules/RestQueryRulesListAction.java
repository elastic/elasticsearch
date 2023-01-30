/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchbusinessrules;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.search.action.QueryRulesListAction;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestQueryRulesListAction extends BaseRestHandler {

    public static final String ENDPOINT = "_query_rules/_list";

    @Override
    public String getName() {
        return "query_rules_list_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/" + ENDPOINT));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        QueryRulesListAction.Request request = new QueryRulesListAction.Request();
        return channel -> client.execute(
            QueryRulesListAction.INSTANCE,
            request,
            new RestToXContentListener<QueryRulesListAction.Response>(channel)
        );
    }
}
