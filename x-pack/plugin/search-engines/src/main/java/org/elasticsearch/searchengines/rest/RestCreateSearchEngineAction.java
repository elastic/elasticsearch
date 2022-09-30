/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.searchengines.rest;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.searchengines.SearchEnginesPlugin;
import org.elasticsearch.searchengines.action.CreateSearchEngineAction;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class RestCreateSearchEngineAction  extends BaseRestHandler {

    @Override
    public String getName() {
        return "create_search_engine_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new RestHandler.Route(PUT, SearchEnginesPlugin.REST_BASE_PATH + "/{name}"));
    }

    @Override
    protected BaseRestHandler.RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        CreateSearchEngineAction.Request createEngineRequest = new CreateSearchEngineAction.Request(request.param("name"));
        return channel -> client.execute(CreateSearchEngineAction.INSTANCE, createEngineRequest, new RestToXContentListener<>(channel));
    }

}
