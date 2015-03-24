/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.rest.action;

import org.elasticsearch.watcher.watch.WatchStore;
import org.elasticsearch.watcher.client.WatcherClient;
import org.elasticsearch.watcher.transport.actions.service.WatcherServiceRequest;
import org.elasticsearch.watcher.transport.actions.service.WatcherServiceResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.support.AcknowledgedRestListener;

/**
 */
public class RestWatchServiceAction extends BaseRestHandler {

    @Inject
    protected RestWatchServiceAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(RestRequest.Method.PUT, WatchStore.INDEX + "/_restart", this);
        controller.registerHandler(RestRequest.Method.PUT, WatchStore.INDEX + "/_start", new StartRestHandler(settings, controller, client));
        controller.registerHandler(RestRequest.Method.PUT, WatchStore.INDEX + "/_stop", new StopRestHandler(settings, controller, client));
    }

    @Override
    protected void handleRequest(RestRequest request, RestChannel channel, Client client) throws Exception {
        new WatcherClient(client).watcherService(new WatcherServiceRequest().restart(), new AcknowledgedRestListener<WatcherServiceResponse>(channel));
    }

    static class StartRestHandler extends BaseRestHandler {

        public StartRestHandler(Settings settings, RestController controller, Client client) {
            super(settings, controller, client);
        }

        @Override
        public void handleRequest(RestRequest request, RestChannel channel, Client client) throws Exception {
            new WatcherClient(client).watcherService(new WatcherServiceRequest().start(), new AcknowledgedRestListener<WatcherServiceResponse>(channel));
        }
    }

    static class StopRestHandler extends BaseRestHandler {

        public StopRestHandler(Settings settings, RestController controller, Client client) {
            super(settings, controller, client);
        }

        @Override
        public void handleRequest(RestRequest request, RestChannel channel, Client client) throws Exception {
            new WatcherClient(client).watcherService(new WatcherServiceRequest().stop(), new AcknowledgedRestListener<WatcherServiceResponse>(channel));
        }
    }
}
