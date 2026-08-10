/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rest;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.inference.action.PutRegionPolicyAction;

import java.io.IOException;
import java.util.List;

@ServerlessScope(Scope.PUBLIC)
public class RestPutRegionPolicyAction extends BaseRestHandler {

    private static final String FORCE_NAME = "force";

    @Override
    public String getName() {
        return "inference_put_region_policy_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.PUT, "/_inference/_region_policy"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        if (request.hasContentOrSourceParam() == false) {
            throw new ElasticsearchParseException("body is required");
        }

        boolean force = request.paramAsBoolean(FORCE_NAME, false);

        try (XContentParser parser = request.contentOrSourceParamParser()) {
            var parsedRequest = PutRegionPolicyAction.Request.parseRequest(parser);
            var requestWithForce = new PutRegionPolicyAction.Request(parsedRequest.regionPolicy(), force);
            return channel -> client.execute(PutRegionPolicyAction.INSTANCE, requestWithForce, new RestToXContentListener<>(channel));
        }
    }
}
