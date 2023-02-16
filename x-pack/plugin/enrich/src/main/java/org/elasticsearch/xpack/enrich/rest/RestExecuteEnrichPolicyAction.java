/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich.rest;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyAction;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class RestExecuteEnrichPolicyAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_enrich/policy/{name}/_execute"), new Route(PUT, "/_enrich/policy/{name}/_execute"));
    }

    @Override
    public String getName() {
        return "execute_enrich_policy";
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest restRequest, final NodeClient client) throws IOException {
        final ExecuteEnrichPolicyAction.Request request = new ExecuteEnrichPolicyAction.Request(restRequest.param("name"));
        request.setWaitForCompletion(restRequest.paramAsBoolean("wait_for_completion", true));
        return channel -> client.execute(ExecuteEnrichPolicyAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
