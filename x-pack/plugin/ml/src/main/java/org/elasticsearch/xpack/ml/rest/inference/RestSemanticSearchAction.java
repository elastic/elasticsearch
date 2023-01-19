/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.rest.inference;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.SemanticSearchAction;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestSemanticSearchAction extends BaseRestHandler {

    public RestSemanticSearchAction() {}

    @Override
    public List<Route> routes() {
        String path = "{index}/_semantic_search";
        return List.of(Route.builder(GET, path).build(), Route.builder(POST, path).build());
    }

    @Override
    public String getName() {
        return "semantic_search_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        SemanticSearchAction.Request request = SemanticSearchAction.Request.parseRestRequest(restRequest);
        return channel -> client.execute(SemanticSearchAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
