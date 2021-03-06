/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.watcher.rest.action;

import org.elasticsearch.common.RestApiVersion;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.watcher.client.WatcherClient;
import org.elasticsearch.xpack.core.watcher.transport.actions.service.WatcherServiceRequest;
import org.elasticsearch.xpack.watcher.rest.WatcherRestHandler;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestWatchServiceAction extends WatcherRestHandler {

    @Override
    public List<Route> routes() {
        return org.elasticsearch.common.collect.List.of(
            Route.builder(POST, "/_watcher/_start")
                .replaces(POST, URI_BASE + "/watcher/_start", RestApiVersion.V_7).build()
        );
    }

    @Override
    public String getName() {
        return "watcher_start_service";
    }

    @Override
    public RestChannelConsumer doPrepareRequest(RestRequest request, WatcherClient client) {
        return channel -> client.watcherService(new WatcherServiceRequest().start(), new RestToXContentListener<>(channel));
    }

    public static class StopRestHandler extends WatcherRestHandler {

        @Override
        public List<Route> routes() {
            return org.elasticsearch.common.collect.List.of(
                Route.builder(POST, "/_watcher/_stop")
                    .replaces(POST, URI_BASE + "/watcher/_stop", RestApiVersion.V_7).build()
            );
        }

        @Override
        public String getName() {
            return "watcher_stop_service";
        }

        @Override
        public RestChannelConsumer doPrepareRequest(RestRequest request, WatcherClient client) {
            return channel -> client.watcherService(new WatcherServiceRequest().stop(), new RestToXContentListener<>(channel));
        }
    }
}
