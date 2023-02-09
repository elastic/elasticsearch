/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.seektracker;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.util.List;

public class RestSeekCountAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "seek_stats_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new RestHandler.Route(RestRequest.Method.GET, "/_seek_stats"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        return channel -> client.executeLocally(SeekStatsAction.INSTANCE, new SeekStatsRequest(), new RestToXContentListener<>(channel));
    }
}
