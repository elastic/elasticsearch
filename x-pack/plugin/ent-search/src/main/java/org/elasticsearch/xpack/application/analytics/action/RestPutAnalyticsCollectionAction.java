/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.application.EnterpriseSearch;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class RestPutAnalyticsCollectionAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "analytics_post_action";
    }

    @Override
    public List<RestHandler.Route> routes() {
        return List.of(new RestHandler.Route(PUT, "/" + EnterpriseSearch.BEHAVIORAL_ANALYTICS_API_ENDPOINT + "/{collection_name}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        PutAnalyticsCollectionAction.Request request = new PutAnalyticsCollectionAction.Request(
            restRequest.param("collection_name"),
            restRequest.content(),
            restRequest.getXContentType()
        );
        return channel -> client.execute(PutAnalyticsCollectionAction.INSTANCE, request, new RestToXContentListener<>(channel) {
            @Override
            protected RestStatus getStatus(PutAnalyticsCollectionAction.Response response) {
                return response.status();
            }
        });
    }

}
