/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich.rest;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;

@ServerlessScope(Scope.INTERNAL)
public class RestEnrichStatsAction extends BaseRestHandler {

    private static final Set<String> SUPPORTED_CAPABILITIES = Set.of("size-in-bytes");

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_enrich/_stats"));
    }

    @Override
    public String getName() {
        return "enrich_stats";
    }

    @Override
    public Set<String> supportedCapabilities() {
        return SUPPORTED_CAPABILITIES;
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest restRequest, final NodeClient client) {
        final var request = new EnrichStatsAction.Request(RestUtils.getMasterNodeTimeout(restRequest));
        return channel -> new RestCancellableNodeClient(client, restRequest.getHttpChannel()).execute(
            EnrichStatsAction.INSTANCE,
            request,
            new RestToXContentListener<>(channel)
        );
    }

}
