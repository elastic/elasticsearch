/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entsearch.analytics.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.entsearch.EnterpriseSearch;
import org.elasticsearch.xpack.entsearch.engine.action.DeleteEngineAction;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

public class RestDeleteAnalyticsCollectionAction extends BaseRestHandler {


    @Override
    public String getName() {
        return "behavioral_analytics_delete_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(DELETE, "/" + EnterpriseSearch.BEHAVIORAL_ANALYTICS_API_ENDPOINT + "/{collection_id}")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        DeleteAnalyticsCollectionAction.Request request = new DeleteAnalyticsCollectionAction.Request(restRequest.param("collection_id"));
        return channel -> client.execute(DeleteEngineAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
