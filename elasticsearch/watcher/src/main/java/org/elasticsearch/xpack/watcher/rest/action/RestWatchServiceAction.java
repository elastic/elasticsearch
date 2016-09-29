/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.rest.action;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.AcknowledgedRestListener;
import org.elasticsearch.xpack.watcher.client.WatcherClient;
import org.elasticsearch.xpack.watcher.rest.WatcherRestHandler;
import org.elasticsearch.xpack.watcher.transport.actions.service.WatcherServiceRequest;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

/**
 */
public class RestWatchServiceAction extends WatcherRestHandler {

    @Inject
    public RestWatchServiceAction(Settings settings, RestController controller) {
        super(settings);

        // @deprecated Remove in 6.0
        // NOTE: we switched from PUT in 2.x to POST in 5.x
        controller.registerWithDeprecatedHandler(POST, URI_BASE + "/_restart", this,
                                                 PUT, "/_watcher/_restart", deprecationLogger);
        controller.registerWithDeprecatedHandler(POST, URI_BASE + "/_start", new StartRestHandler(settings),
                                                 PUT, "/_watcher/_start", deprecationLogger);
        controller.registerWithDeprecatedHandler(POST, URI_BASE + "/_stop", new StopRestHandler(settings),
                                                 PUT, "/_watcher/_stop", deprecationLogger);
    }

    @Override
    public void handleRequest(RestRequest request, RestChannel channel, WatcherClient client) throws Exception {
        client.watcherService(new WatcherServiceRequest().restart(), new AcknowledgedRestListener<>(channel));
    }

    static class StartRestHandler extends WatcherRestHandler {

        public StartRestHandler(Settings settings) {
            super(settings);
        }

        @Override
        public void handleRequest(RestRequest request, RestChannel channel, WatcherClient client) throws Exception {
            client.watcherService(new WatcherServiceRequest().start(), new AcknowledgedRestListener<>(channel));
        }
    }

    static class StopRestHandler extends WatcherRestHandler {

        public StopRestHandler(Settings settings) {
            super(settings);
        }

        @Override
        public void handleRequest(RestRequest request, RestChannel channel, WatcherClient client) throws Exception {
            client.watcherService(new WatcherServiceRequest().stop(), new AcknowledgedRestListener<>(channel));
        }
    }
}
