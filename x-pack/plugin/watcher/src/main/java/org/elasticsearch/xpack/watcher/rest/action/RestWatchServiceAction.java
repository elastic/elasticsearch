/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.watcher.rest.action;

import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.watcher.client.WatcherClient;
import org.elasticsearch.xpack.core.watcher.transport.actions.service.WatcherServiceRequest;
import org.elasticsearch.xpack.watcher.rest.WatcherRestHandler;

import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestWatchServiceAction extends WatcherRestHandler {

    @Override
    public List<Route> routes() {
        return emptyList();
    }

    @Override
    public List<ReplacedRoute> replacedRoutes() {
        return singletonList(new ReplacedRoute(POST, "/_watcher/_start", POST, URI_BASE + "/watcher/_start"));
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
            return emptyList();
        }

        @Override
        public List<ReplacedRoute> replacedRoutes() {
            return singletonList(new ReplacedRoute(POST, "/_watcher/_stop", POST, URI_BASE + "/watcher/_stop"));
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
