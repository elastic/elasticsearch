/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.textstructure.rest;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.textstructure.action.TestGrokPatternAction;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.textstructure.TextStructurePlugin.BASE_PATH;

@ServerlessScope(Scope.INTERNAL)
public class RestTestGrokPatternAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "text_structure_test_grok_pattern_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, BASE_PATH + "test_grok_pattern"), new Route(POST, BASE_PATH + "test_grok_pattern"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        TestGrokPatternAction.Request request;
        String ecsCompatibility = restRequest.param(TestGrokPatternAction.Request.ECS_COMPATIBILITY.getPreferredName());
        try (XContentParser parser = restRequest.contentOrSourceParamParser()) {
            request = TestGrokPatternAction.Request.parseRequest(ecsCompatibility, parser);
        }

        return channel -> client.execute(TestGrokPatternAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
