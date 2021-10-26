/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.rest.action;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.watcher.transport.actions.QueryWatchesAction;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestQueryWatchesAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_watcher/_query/watches"),
            new Route(POST, "/_watcher/_query/watches")
        );
    }

    @Override
    public String getName() {
        return "watcher_query_watches";
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, NodeClient client) throws IOException {
        final QueryWatchesAction.Request queryWatchesRequest;
        if (request.hasContentOrSourceParam()) {
            queryWatchesRequest = QueryWatchesAction.Request.fromXContent(request.contentOrSourceParamParser());
        } else {
            queryWatchesRequest = new QueryWatchesAction.Request(null, null, null, null, null);
        }
        return channel -> client.execute(QueryWatchesAction.INSTANCE, queryWatchesRequest, new RestToXContentListener<>(channel));
    }
}
