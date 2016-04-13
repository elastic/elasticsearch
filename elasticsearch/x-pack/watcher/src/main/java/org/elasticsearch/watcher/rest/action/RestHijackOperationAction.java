/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.rest.action;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.watcher.client.WatcherClient;
import org.elasticsearch.watcher.rest.WatcherRestHandler;
import org.elasticsearch.watcher.watch.WatchStore;

/**
  */
public class RestHijackOperationAction extends WatcherRestHandler {
    private static String ALLOW_DIRECT_ACCESS_TO_WATCH_INDEX_SETTING = "xpack.watcher.index.rest.direct_access";


    @Inject
    public RestHijackOperationAction(Settings settings, RestController controller, Client client) {
        super(settings, client);
        if (!settings.getAsBoolean(ALLOW_DIRECT_ACCESS_TO_WATCH_INDEX_SETTING, false)) {
            WatcherRestHandler unsupportedHandler = new UnsupportedHandler(settings, client);
            controller.registerHandler(RestRequest.Method.POST, WatchStore.INDEX + "/watch", this);
            controller.registerHandler(RestRequest.Method.POST, WatchStore.INDEX + "/watch/{id}", this);
            controller.registerHandler(RestRequest.Method.PUT, WatchStore.INDEX + "/watch/{id}", this);
            controller.registerHandler(RestRequest.Method.POST, WatchStore.INDEX + "/watch/{id}/_update", this);
            controller.registerHandler(RestRequest.Method.DELETE, WatchStore.INDEX + "/watch/_query", this);
            controller.registerHandler(RestRequest.Method.DELETE, WatchStore.INDEX + "/watch/{id}", this);
            controller.registerHandler(RestRequest.Method.GET, WatchStore.INDEX + "/watch/{id}", this);
            controller.registerHandler(RestRequest.Method.POST, WatchStore.INDEX + "/watch/_bulk", unsupportedHandler);
            controller.registerHandler(RestRequest.Method.POST, WatchStore.INDEX + "/_bulk", unsupportedHandler);
            controller.registerHandler(RestRequest.Method.PUT, WatchStore.INDEX + "/watch/_bulk", unsupportedHandler);
            controller.registerHandler(RestRequest.Method.PUT, WatchStore.INDEX + "/_bulk", unsupportedHandler);
            controller.registerHandler(RestRequest.Method.DELETE, WatchStore.INDEX, unsupportedHandler);
        }
    }

    @Override
    protected void handleRequest(RestRequest request, RestChannel channel, WatcherClient client) throws Exception {
        XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
        jsonBuilder.startObject().field("error","This endpoint is not supported for " +
                request.method().name() + " on " + WatchStore.INDEX + " index. Please use " +
                request.method().name() + " " + URI_BASE + "/watch/<watch_id> instead");
        jsonBuilder.field("status", RestStatus.BAD_REQUEST.getStatus());
        jsonBuilder.endObject();
        channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, jsonBuilder));
    }

    public static class UnsupportedHandler extends WatcherRestHandler {

        public UnsupportedHandler(Settings settings, Client client) {
            super(settings, client);
        }

        @Override
        protected void handleRequest(RestRequest request, RestChannel channel, WatcherClient client) throws Exception {
            request.path();
            XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
            jsonBuilder.startObject().field("error","This endpoint is not supported for " +
                    request.method().name() + " on " + WatchStore.INDEX + " index.");
            jsonBuilder.field("status", RestStatus.BAD_REQUEST.getStatus());
            jsonBuilder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, jsonBuilder));
        }
    }
}
