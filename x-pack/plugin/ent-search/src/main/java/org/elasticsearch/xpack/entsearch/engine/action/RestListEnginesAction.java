/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entsearch.engine.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.entsearch.EnterpriseSearch;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestListEnginesAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "engines_list_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/" + EnterpriseSearch.ENGINE_API_ENDPOINT));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {

        int from = restRequest.paramAsInt("from", PageParams.DEFAULT_FROM);
        int size = restRequest.paramAsInt("size", PageParams.DEFAULT_SIZE);
        ListEnginesAction.Request request = new ListEnginesAction.Request(restRequest.param("q"), new PageParams(from, size));

        return channel -> client.execute(ListEnginesAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
