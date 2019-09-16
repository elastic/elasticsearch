/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ccr.action.FollowStatsAction;

public class RestFollowStatsAction extends BaseRestHandler {

    public RestFollowStatsAction(final RestController controller) {
        controller.registerHandler(RestRequest.Method.GET, "/{index}/_ccr/stats", this);
    }

    @Override
    public String getName() {
        return "ccr_follower_stats";
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest restRequest, final NodeClient client) {
        final FollowStatsAction.StatsRequest request = new FollowStatsAction.StatsRequest();
        request.setIndices(Strings.splitStringByCommaToArray(restRequest.param("index")));
        return channel -> client.execute(FollowStatsAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }

}
