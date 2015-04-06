/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.rest.action;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.support.AcknowledgedRestListener;
import org.elasticsearch.watcher.client.WatcherClient;
import org.elasticsearch.watcher.rest.WatcherRestHandler;
import org.elasticsearch.watcher.transport.actions.service.WatcherServiceRequest;
import org.elasticsearch.watcher.transport.actions.service.WatcherServiceResponse;

/**
 */
public class RestWatchServiceAction extends WatcherRestHandler {

    private final WatcherClient watcherClient;

    @Inject
    protected RestWatchServiceAction(Settings settings, RestController controller, Client client, WatcherClient watcherClient) {
        super(settings, controller, client);
        controller.registerHandler(RestRequest.Method.PUT, URI_BASE + "/_restart", this);
        controller.registerHandler(RestRequest.Method.PUT, URI_BASE + "/_start", new StartRestHandler(settings, controller, client, watcherClient));
        controller.registerHandler(RestRequest.Method.PUT, URI_BASE + "/_stop", new StopRestHandler(settings, controller, client, watcherClient));
        this.watcherClient = watcherClient;
    }

    @Override
    protected void handleRequest(RestRequest request, RestChannel channel, WatcherClient client) throws Exception {
        watcherClient.watcherService(new WatcherServiceRequest().restart(), new AcknowledgedRestListener<WatcherServiceResponse>(channel));
    }

    static class StartRestHandler extends BaseRestHandler {

        private final WatcherClient watcherClient;

        public StartRestHandler(Settings settings, RestController controller, Client client, WatcherClient watcherClient) {
            super(settings, controller, client);
            this.watcherClient = watcherClient;
        }

        @Override
        public void handleRequest(RestRequest request, RestChannel channel, Client client) throws Exception {
            watcherClient.watcherService(new WatcherServiceRequest().start(), new AcknowledgedRestListener<WatcherServiceResponse>(channel));
        }
    }

    static class StopRestHandler extends BaseRestHandler {

        private final WatcherClient watcherClient;

        public StopRestHandler(Settings settings, RestController controller, Client client, WatcherClient watcherClient) {
            super(settings, controller, client);
            this.watcherClient = watcherClient;
        }

        @Override
        public void handleRequest(RestRequest request, RestChannel channel, Client client) throws Exception {
            watcherClient.watcherService(new WatcherServiceRequest().stop(), new AcknowledgedRestListener<WatcherServiceResponse>(channel));
        }
    }
}
