/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchbusinessrules;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.search.action.QueryRulesDeleteAction;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

public class RestQueryRulesDeleteAction extends BaseRestHandler {

    public static final String ENDPOINT = "_query_rules";

    @Override
    public String getName() {
        return "query_rules_delete_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(DELETE, "/" + ENDPOINT + "/{ruleset_id}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        QueryRulesDeleteAction.Request request = new QueryRulesDeleteAction.Request(restRequest.param("ruleset_id"));
        return channel -> client.execute(
            QueryRulesDeleteAction.INSTANCE,
            request,
            new RestToXContentListener<AcknowledgedResponse>(channel)
        );
    }
}
