/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.rest.action;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
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

    @Inject
    public RestWatchServiceAction(Settings settings, RestController controller, Client client) {
        super(settings, client);
        controller.registerHandler(RestRequest.Method.PUT, URI_BASE + "/_restart", this);
        controller.registerHandler(RestRequest.Method.PUT, URI_BASE + "/_start", new StartRestHandler(settings, client));
        controller.registerHandler(RestRequest.Method.PUT, URI_BASE + "/_stop", new StopRestHandler(settings, client));
    }

    @Override
    protected void handleRequest(RestRequest request, RestChannel channel, WatcherClient client) throws Exception {
        client.watcherService(new WatcherServiceRequest().restart(), new AcknowledgedRestListener<WatcherServiceResponse>(channel));
    }

    static class StartRestHandler extends WatcherRestHandler {

        public StartRestHandler(Settings settings, Client client) {
            super(settings, client);
        }

        @Override
        protected void handleRequest(RestRequest request, RestChannel channel, WatcherClient client) throws Exception {
            client.watcherService(new WatcherServiceRequest().start(), new AcknowledgedRestListener<WatcherServiceResponse>(channel));
        }
    }

    static class StopRestHandler extends WatcherRestHandler {

        public StopRestHandler(Settings settings, Client client) {
            super(settings, client);
        }

        @Override
        protected void handleRequest(RestRequest request, RestChannel channel, WatcherClient client) throws Exception {
            client.watcherService(new WatcherServiceRequest().stop(), new AcknowledgedRestListener<WatcherServiceResponse>(channel));
        }
    }
}
