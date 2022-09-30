/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.searchengines.rest;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.searchengines.SearchEnginesPlugin;
import org.elasticsearch.searchengines.action.GetSearchEngineAction;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;;

public class RestGetSearchEngineAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "get_search_engine_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new RestHandler.Route(GET, SearchEnginesPlugin.REST_BASE_PATH + "/{name}")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        GetSearchEngineAction.Request getContentIndexRequest = new GetSearchEngineAction.Request(
            Strings.splitStringByCommaToArray(request.param("name"))
        );
        getContentIndexRequest.indicesOptions(IndicesOptions.fromRequest(request, getContentIndexRequest.indicesOptions()));
        return channel -> client.execute(GetSearchEngineAction.INSTANCE, getContentIndexRequest, new RestToXContentListener<>(channel));
    }

    @Override
    public boolean allowSystemIndexAccessByDefault() {
        return false;
    }
}
