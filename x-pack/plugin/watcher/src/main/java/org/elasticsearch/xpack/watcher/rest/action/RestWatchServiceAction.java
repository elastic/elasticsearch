/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.rest.action;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.watcher.client.WatcherClient;
import org.elasticsearch.xpack.core.watcher.transport.actions.service.WatcherServiceRequest;
import org.elasticsearch.xpack.watcher.rest.WatcherRestHandler;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class RestWatchServiceAction extends WatcherRestHandler {
    private static final Logger logger = LogManager.getLogger(RestWatchServiceAction.class);
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(logger);

    public RestWatchServiceAction(Settings settings, RestController controller) {
        super(settings);

        // NOTE: we switched from PUT in 2.x to POST in 5.x
        // NOTE: We added back the old URL with the new VERB (POST) since we are deprecating _xpack/* URIs in 7.0
        controller.registerWithDeprecatedHandler(POST, URI_BASE + "/_restart", this,
                PUT, "/_watcher/_restart", deprecationLogger);
        StartRestHandler startRestHandler = new StartRestHandler(settings);
        controller.registerWithDeprecatedHandler(POST, URI_BASE + "/_start",
                startRestHandler, PUT, "/_watcher/_start", deprecationLogger);
        controller.registerHandler(POST, "/_watcher/_start", startRestHandler);
        StopRestHandler stopRestHandler = new StopRestHandler(settings);
        controller.registerWithDeprecatedHandler(POST, URI_BASE + "/_stop",
                stopRestHandler, PUT, "/_watcher/_stop", deprecationLogger);
        controller.registerHandler(POST, "/_watcher/_stop", stopRestHandler);
    }

    @Override
    public String getName() {
        return "xpack_watcher_service_action";
    }

    @Override
    public RestChannelConsumer doPrepareRequest(RestRequest request, WatcherClient client) {
        return channel -> client.watcherService(new WatcherServiceRequest().stop(),
                ActionListener.wrap(
                    stopResponse -> client.watcherService(new WatcherServiceRequest().start(), new RestToXContentListener<>(channel)),
                    e -> {
                        try {
                            channel.sendResponse(new BytesRestResponse(channel, e));
                        } catch (Exception inner) {
                            inner.addSuppressed(e);
                            logger.error("failed to send failure response", inner);
                        }
                    }));
    }

    private static class StartRestHandler extends WatcherRestHandler {

        StartRestHandler(Settings settings) {
            super(settings);
        }

        @Override
        public String getName() {
            return "xpack_watcher_start_service_action";
        }

        @Override
        public RestChannelConsumer doPrepareRequest(RestRequest request, WatcherClient client) {
            return channel -> client.watcherService(new WatcherServiceRequest().start(), new RestToXContentListener<>(channel));
        }
    }

    private static class StopRestHandler extends WatcherRestHandler {

        StopRestHandler(Settings settings) {
            super(settings);
        }

        @Override
        public String getName() {
            return "xpack_watcher_stop_service_action";
        }

        @Override
        public RestChannelConsumer doPrepareRequest(RestRequest request, WatcherClient client) {
            return channel -> client.watcherService(new WatcherServiceRequest().stop(), new RestToXContentListener<>(channel));
        }
    }
}
