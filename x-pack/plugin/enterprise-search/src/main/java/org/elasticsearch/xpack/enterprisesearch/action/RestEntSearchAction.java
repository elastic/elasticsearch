/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.enterprisesearch.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestStatusToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestEntSearchAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "rest_ent_search_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/{index}/_entsearch"),
            new Route(POST, "/{index}/_entsearch")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {

        EntSearchRequest entSearchRequest = new EntSearchRequest();
        entSearchRequest.setIndex(request.param("index"));
        entSearchRequest.setQuery(request.param("query"));

        return channel -> client.execute(EntSearchAction.INSTANCE, entSearchRequest, new RestStatusToXContentListener<>(channel));
    }
}
