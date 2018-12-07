/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.rest.action;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.core.watcher.client.WatcherClient;
import org.elasticsearch.xpack.core.watcher.support.xcontent.WatcherParams;
import org.elasticsearch.xpack.core.watcher.transport.actions.activate.ActivateWatchRequest;
import org.elasticsearch.xpack.core.watcher.transport.actions.activate.ActivateWatchResponse;
import org.elasticsearch.xpack.core.watcher.watch.WatchField;
import org.elasticsearch.xpack.watcher.rest.WatcherRestHandler;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

/**
 * The rest action to de/activate a watch
 */
public class RestActivateWatchAction extends WatcherRestHandler {
    private static final Logger logger = LogManager.getLogger(RestActivateWatchAction.class);
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(logger);

    public RestActivateWatchAction(Settings settings, RestController controller) {
        super(settings);

        final DeactivateRestHandler deactivateRestHandler = new DeactivateRestHandler(settings);

        controller.registerHandler(POST, URI_BASE + "/watch/{id}/_activate", this);
        controller.registerHandler(POST, "/_watcher/watch/{id}/_activate", this);
        controller.registerHandler(PUT, URI_BASE + "/watch/{id}/_activate", this);
        controller.registerHandler(PUT, "/_watcher/watch/{id}/_activate", this);
        controller.registerHandler(POST, URI_BASE + "/watch/{id}/_deactivate", deactivateRestHandler);
        controller.registerHandler(POST, "/_watcher/watch/{id}/_deactivate", deactivateRestHandler);
        controller.registerHandler(PUT, URI_BASE + "/watch/{id}/_deactivate", deactivateRestHandler);
        controller.registerHandler(PUT, "/_watcher/watch/{id}/_deactivate", deactivateRestHandler);
    }

    @Override
    public String getName() {
        return "xpack_watcher_activate_watch_action";
    }

    @Override
    public RestChannelConsumer doPrepareRequest(RestRequest request, WatcherClient client) throws IOException {
        String watchId = request.param("id");
        return channel ->
                client.activateWatch(new ActivateWatchRequest(watchId, true), new RestBuilderListener<ActivateWatchResponse>(channel) {
                    @Override
                    public RestResponse buildResponse(ActivateWatchResponse response, XContentBuilder builder) throws Exception {
                        return new BytesRestResponse(RestStatus.OK, builder.startObject()
                                .field(WatchField.STATUS.getPreferredName(), response.getStatus(), WatcherParams.HIDE_SECRETS)
                                .endObject());
                    }
                });
    }

    private static class DeactivateRestHandler extends WatcherRestHandler {

        DeactivateRestHandler(Settings settings) {
            super(settings);
        }

        @Override
        public String getName() {
            return "xpack_watcher_deactivate_watch_action";
        }

        @Override
        public RestChannelConsumer doPrepareRequest(RestRequest request, WatcherClient client) throws IOException {
            String watchId = request.param("id");
            return channel ->
                    client.activateWatch(new ActivateWatchRequest(watchId, false), new RestBuilderListener<ActivateWatchResponse>(channel) {
                        @Override
                        public RestResponse buildResponse(ActivateWatchResponse response, XContentBuilder builder) throws Exception {
                            return new BytesRestResponse(RestStatus.OK, builder.startObject()
                                    .field(WatchField.STATUS.getPreferredName(), response.getStatus(), WatcherParams.HIDE_SECRETS)
                                    .endObject());
                        }
                    });
        }
    }

}
