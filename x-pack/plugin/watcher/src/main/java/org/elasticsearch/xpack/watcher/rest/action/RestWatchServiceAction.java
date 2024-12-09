/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.watcher.rest.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.watcher.transport.actions.service.WatcherServiceAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.service.WatcherServiceRequest;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestWatchServiceAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_watcher/_start"));
    }

    @Override
    public String getName() {
        return "watcher_start_service";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        final var req = new WatcherServiceRequest(RestUtils.getMasterNodeTimeout(request)).start();
        return channel -> client.execute(WatcherServiceAction.INSTANCE, req, new RestToXContentListener<>(channel));
    }

    public static class StopRestHandler extends BaseRestHandler {

        @Override
        public List<Route> routes() {
            return List.of(new Route(POST, "/_watcher/_stop"));
        }

        @Override
        public String getName() {
            return "watcher_stop_service";
        }

        @Override
        public RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
            final var request = new WatcherServiceRequest(RestUtils.getMasterNodeTimeout(restRequest)).stop();
            return channel -> client.execute(WatcherServiceAction.INSTANCE, request, new RestToXContentListener<>(channel));
        }
    }
}
