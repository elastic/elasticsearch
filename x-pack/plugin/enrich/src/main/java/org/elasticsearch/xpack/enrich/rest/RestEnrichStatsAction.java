/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction;

import java.io.IOException;

public class RestEnrichStatsAction extends BaseRestHandler {

    public RestEnrichStatsAction(final RestController controller) {
        controller.registerHandler(RestRequest.Method.GET, "/_enrich/_stats", this);
    }

    @Override
    public String getName() {
        return "enrich_stats";
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest restRequest, final NodeClient client) throws IOException {
        final EnrichStatsAction.Request request = new EnrichStatsAction.Request();
        return channel -> client.execute(EnrichStatsAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }

}
