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
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.entsearch.EnterpriseSearch;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

public class RestDeleteEngineAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "engine_delete_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(DELETE, "/" + EnterpriseSearch.ENGINE_API_ENDPOINT + "/{engine_id}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        DeleteEngineAction.Request request = new DeleteEngineAction.Request(restRequest.param("engine_id"));
        return channel -> client.execute(PutEngineAction.INSTANCE, request, new RestToXContentListener<>(channel) {
            @Override
            protected RestStatus getStatus(PutEngineAction.Response response) {
                return response.status();
            }
        });
    }
}
