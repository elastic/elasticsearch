/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.watcher.rest.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.watcher.transport.actions.service.WatcherServiceAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.service.WatcherServiceRequest;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestWatchServiceAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(Route.builder(POST, "/_watcher/_start").replaces(POST, "/_xpack/watcher/_start", RestApiVersion.V_7).build());
    }

    @Override
    public String getName() {
        return "watcher_start_service";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        return channel -> client.execute(
            WatcherServiceAction.INSTANCE,
            new WatcherServiceRequest().start(),
            new RestToXContentListener<>(channel)
        );
    }

    public static class StopRestHandler extends BaseRestHandler {

        @Override
        public List<Route> routes() {
            return List.of(Route.builder(POST, "/_watcher/_stop").replaces(POST, "/_xpack/watcher/_stop", RestApiVersion.V_7).build());
        }

        @Override
        public String getName() {
            return "watcher_stop_service";
        }

        @Override
        public RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
            final WatcherServiceRequest request = new WatcherServiceRequest().stop();
            request.masterNodeTimeout(restRequest.paramAsTime("master_timeout", request.masterNodeTimeout()));
            return channel -> client.execute(WatcherServiceAction.INSTANCE, request, new RestToXContentListener<>(channel));
        }
    }
}
