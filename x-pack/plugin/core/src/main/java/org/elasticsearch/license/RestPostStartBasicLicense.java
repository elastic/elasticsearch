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

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestUtils.getAckTimeout;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;

public class RestPostStartBasicLicense extends BaseRestHandler {

    public RestPostStartBasicLicense() {}

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_license/start_basic"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        PostStartBasicRequest startBasicRequest = new PostStartBasicRequest(getMasterNodeTimeout(request), getAckTimeout(request));
        startBasicRequest.acknowledge(request.paramAsBoolean("acknowledge", false));
        return channel -> client.execute(
            PostStartBasicAction.INSTANCE,
            startBasicRequest,
            new RestToXContentListener<>(channel, PostStartBasicResponse::status)
        );
    }

    @Override
    public String getName() {
        return "post_start_basic";
    }

}
