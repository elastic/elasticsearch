/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.rest.action;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.watcher.transport.actions.ListWatchesAction;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestListWatchesAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_watcher/_list_watches"),
            new Route(POST, "/_watcher/_list_watches")
        );
    }

    @Override
    public String getName() {
        return "watcher_list_watches";
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, NodeClient client) throws IOException {
        final ListWatchesAction.Request listWatchesRequest;
        if (request.hasContentOrSourceParam()) {
            listWatchesRequest = ListWatchesAction.Request.fromXContent(request.contentOrSourceParamParser());
        } else {
            listWatchesRequest = new ListWatchesAction.Request(null, null, null, null);
        }
        return channel -> client.execute(ListWatchesAction.INSTANCE, listWatchesRequest, new RestToXContentListener<>(channel));
    }
}
