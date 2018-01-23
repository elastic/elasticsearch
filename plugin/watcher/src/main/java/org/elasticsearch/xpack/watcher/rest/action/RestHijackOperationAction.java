/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.rest.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.watcher.client.WatcherClient;
import org.elasticsearch.xpack.watcher.rest.WatcherRestHandler;
import org.elasticsearch.xpack.core.watcher.watch.Watch;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class RestHijackOperationAction extends WatcherRestHandler {

    private static final String ALLOW_DIRECT_ACCESS_TO_WATCH_INDEX_SETTING = "xpack.watcher.index.rest.direct_access";

    public RestHijackOperationAction(Settings settings, RestController controller) {
        super(settings);
        if (!settings.getAsBoolean(ALLOW_DIRECT_ACCESS_TO_WATCH_INDEX_SETTING, false)) {
            WatcherRestHandler unsupportedHandler = new UnsupportedHandler(settings);
            controller.registerHandler(POST, Watch.INDEX + "/watch", this);
            controller.registerHandler(POST, Watch.INDEX + "/watch/{id}", this);
            controller.registerHandler(PUT, Watch.INDEX + "/watch/{id}", this);
            controller.registerHandler(POST, Watch.INDEX + "/watch/{id}/_update", this);
            controller.registerHandler(DELETE, Watch.INDEX + "/watch/_query", this);
            controller.registerHandler(DELETE, Watch.INDEX + "/watch/{id}", this);
            controller.registerHandler(GET, Watch.INDEX + "/watch/{id}", this);
            controller.registerHandler(POST, Watch.INDEX + "/watch/_bulk", unsupportedHandler);
            controller.registerHandler(POST, Watch.INDEX + "/_bulk", unsupportedHandler);
            controller.registerHandler(PUT, Watch.INDEX + "/watch/_bulk", unsupportedHandler);
            controller.registerHandler(PUT, Watch.INDEX + "/_bulk", unsupportedHandler);
            controller.registerHandler(DELETE, Watch.INDEX, unsupportedHandler);
            controller.registerHandler(POST, Watch.INDEX + "/_delete_by_query", unsupportedHandler);
            controller.registerHandler(POST, Watch.INDEX + "/watch/_delete_by_query", unsupportedHandler);
            controller.registerHandler(POST, Watch.INDEX + "/_update_by_query", unsupportedHandler);
            controller.registerHandler(POST, Watch.INDEX + "/watch/_update_by_query", unsupportedHandler);
        }
    }

    @Override
    public String getName() {
        return "xpack_watcher_hijack_operation_action";
    }

    @Override
    public RestChannelConsumer doPrepareRequest(RestRequest request, WatcherClient client) throws IOException {
        // we have to consume the id parameter lest the request will fail for the wrong reason
        if (request.hasParam("id")) {
            request.param("id");
        }
        return channel -> {
            try (XContentBuilder builder = channel.newErrorBuilder()) {
                builder.startObject().field("error", "This endpoint is not supported for " +
                        request.method().name() + " on " + Watch.INDEX + " index. Please use " +
                        request.method().name() + " " + URI_BASE + "/watch/<watch_id> instead");
                builder.field("status", RestStatus.BAD_REQUEST.getStatus());
                builder.endObject();
                channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, builder));
            }
        };
    }

    private static class UnsupportedHandler extends WatcherRestHandler {

        private UnsupportedHandler(Settings settings) {
            super(settings);
        }

        @Override
        public String getName() {
            return "xpack_watcher_unsupported_action";
        }

        @Override
        public RestChannelConsumer doPrepareRequest(RestRequest request, WatcherClient client) throws IOException {
            // we have to consume the id parameter lest the request will fail for the wrong reason
            if (request.hasParam("id")) {
                request.param("id");
            }

            return channel -> {
                try (XContentBuilder builder = channel.newErrorBuilder()) {
                    builder.startObject().field("error", "This endpoint is not supported for " +
                            request.method().name() + " on " + Watch.INDEX + " index.");
                    builder.field("status", RestStatus.BAD_REQUEST.getStatus());
                    builder.endObject();
                    channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, builder));
                }
            };
        }
    }
}
