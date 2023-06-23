/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr.rest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestChunkedToXContentListener;
import org.elasticsearch.xpack.ccr.Ccr;
import org.elasticsearch.xpack.core.ccr.action.CcrStatsAction;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestCcrStatsAction extends BaseRestHandler {

    private static final Logger logger = LogManager.getLogger(RestCcrStatsAction.class);

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_ccr/stats"));
    }

    @Override
    public String getName() {
        return "ccr_stats";
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest restRequest, final NodeClient client) {
        final CcrStatsAction.Request request = new CcrStatsAction.Request();
        return channel -> client.execute(
            CcrStatsAction.INSTANCE,
            request,
            new ThreadedActionListener<>(
                client.threadPool().executor(Ccr.CCR_THREAD_POOL_NAME),
                new RestChunkedToXContentListener<>(channel)
            )
        );
    }

}
