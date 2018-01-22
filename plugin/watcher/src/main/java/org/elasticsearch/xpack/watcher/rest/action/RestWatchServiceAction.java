/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.rest.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.AcknowledgedRestListener;
import org.elasticsearch.xpack.watcher.client.WatcherClient;
import org.elasticsearch.xpack.watcher.rest.WatcherRestHandler;
import org.elasticsearch.xpack.watcher.transport.actions.service.WatcherServiceRequest;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestWatchServiceAction extends WatcherRestHandler {

    public RestWatchServiceAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(POST, URI_BASE + "/_start", this);
        controller.registerHandler(POST, URI_BASE + "/_stop", new StopRestHandler(settings));
    }

    @Override
    public String getName() {
        return "xpack_watcher_start_service_action";
    }

    @Override
    public RestChannelConsumer doPrepareRequest(RestRequest request, WatcherClient client)
            throws IOException {
        return channel -> client.watcherService(new WatcherServiceRequest().start(),
                new AcknowledgedRestListener<>(channel));
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
        public RestChannelConsumer doPrepareRequest(RestRequest request, WatcherClient client)
                throws IOException {
            return channel -> client.watcherService(new WatcherServiceRequest().stop(), new
                    AcknowledgedRestListener<>(channel));
        }
    }
}
