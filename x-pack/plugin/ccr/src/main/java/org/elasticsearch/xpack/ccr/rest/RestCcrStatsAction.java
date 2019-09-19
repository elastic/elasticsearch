/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ccr.action.CcrStatsAction;

public class RestCcrStatsAction extends BaseRestHandler {

    public RestCcrStatsAction(final RestController controller) {
        controller.registerHandler(RestRequest.Method.GET, "/_ccr/stats", this);
    }

    @Override
    public String getName() {
        return "ccr_stats";
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest restRequest, final NodeClient client) {
        final CcrStatsAction.Request request = new CcrStatsAction.Request();
        return channel -> client.execute(CcrStatsAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }

}
