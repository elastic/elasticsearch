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
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestRefCountedChunkedToXContentListener;
import org.elasticsearch.xpack.ccr.Ccr;
import org.elasticsearch.xpack.core.ccr.action.FollowStatsAction;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestFollowStatsAction extends BaseRestHandler {

    private static final Logger logger = LogManager.getLogger(RestFollowStatsAction.class);

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/{index}/_ccr/stats"));
    }

    @Override
    public String getName() {
        return "ccr_follower_stats";
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest restRequest, final NodeClient client) {
        final FollowStatsAction.StatsRequest request = new FollowStatsAction.StatsRequest();
        request.setIndices(Strings.splitStringByCommaToArray(restRequest.param("index")));
        request.setTimeout(restRequest.paramAsTime("timeout", request.getTimeout()));
        return channel -> client.execute(
            FollowStatsAction.INSTANCE,
            request,
            new ThreadedActionListener<>(
                client.threadPool().executor(Ccr.CCR_THREAD_POOL_NAME),
                new RestRefCountedChunkedToXContentListener<>(channel)
            )
        );
    }

}
