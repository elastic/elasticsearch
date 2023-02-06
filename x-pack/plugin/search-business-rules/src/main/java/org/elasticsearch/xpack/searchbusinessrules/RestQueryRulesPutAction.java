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
import org.elasticsearch.xpack.core.search.action.QueryRulesPutAction;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class RestQueryRulesPutAction extends BaseRestHandler {

    public static final String ENDPOINT = "_query_rules";

    @Override
    public String getName() {
        return "query_rules_put_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, "/" + ENDPOINT + "/{ruleset_id}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        QueryRulesPutAction.Request request = new QueryRulesPutAction.Request(
            restRequest.param("ruleset_id"),
            restRequest.content(),
            restRequest.getXContentType()
        );
        return channel -> client.execute(QueryRulesPutAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
