/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.enrich.action.PutEnrichPolicyAction;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class RestPutEnrichPolicyAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, "/_enrich/policy/{name}"));
    }

    @Override
    public String getName() {
        return "put_enrich_policy";
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest restRequest, final NodeClient client) throws IOException {
        final PutEnrichPolicyAction.Request request = createRequest(restRequest);
        return channel -> client.execute(PutEnrichPolicyAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }

    static PutEnrichPolicyAction.Request createRequest(RestRequest restRequest) throws IOException {
        try (XContentParser parser = restRequest.contentOrSourceParamParser()) {
            return PutEnrichPolicyAction.fromXContent(parser, restRequest.param("name"));
        }
    }
}
