/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.license;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestGetFeatureUsageAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "get_feature_usage";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_license/feature_usage"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        return channel -> client.execute(
            TransportGetFeatureUsageAction.TYPE,
            new GetFeatureUsageRequest(),
            new RestToXContentListener<>(channel)
        );
    }
}
