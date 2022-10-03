/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.searchengines.rest;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.searchengines.SearchEnginesPlugin;
import org.elasticsearch.searchengines.action.DeleteSearchEngineAction;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

public class RestDeleteSearchEngineAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "delete_search_engine_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(DELETE, SearchEnginesPlugin.REST_BASE_PATH + "/{name}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        DeleteSearchEngineAction.Request deleteRequest = new DeleteSearchEngineAction.Request(
            Strings.splitStringByCommaToArray(request.param("name"))
        );
        return channel -> client.execute(DeleteSearchEngineAction.INSTANCE, deleteRequest, new RestToXContentListener<>(channel));
    }
}
