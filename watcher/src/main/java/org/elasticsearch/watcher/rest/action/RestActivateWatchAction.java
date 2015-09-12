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
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestBuilderListener;
import org.elasticsearch.watcher.client.WatcherClient;
import org.elasticsearch.watcher.rest.WatcherRestHandler;
import org.elasticsearch.watcher.support.xcontent.WatcherParams;
import org.elasticsearch.watcher.transport.actions.activate.ActivateWatchRequest;
import org.elasticsearch.watcher.transport.actions.activate.ActivateWatchResponse;
import org.elasticsearch.watcher.watch.Watch;

/**
 * The rest action to de/activate a watch
 */
public class RestActivateWatchAction extends WatcherRestHandler {

    @Inject
    public RestActivateWatchAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(RestRequest.Method.PUT, URI_BASE + "/watch/{id}/_activate", this);
        controller.registerHandler(RestRequest.Method.POST, URI_BASE + "/watch/{id}/_activate", this);
        DeactivateRestHandler deactivateRestHandler = new DeactivateRestHandler(settings, controller, client);
        controller.registerHandler(RestRequest.Method.PUT, URI_BASE + "/watch/{id}/_deactivate", deactivateRestHandler);
        controller.registerHandler(RestRequest.Method.POST, URI_BASE + "/watch/{id}/_deactivate", deactivateRestHandler);
    }

    @Override
    protected void handleRequest(RestRequest request, RestChannel channel, WatcherClient client) throws Exception {
        String watchId = request.param("id");
        client.activateWatch(new ActivateWatchRequest(watchId, true), new RestBuilderListener<ActivateWatchResponse>(channel) {
            @Override
            public RestResponse buildResponse(ActivateWatchResponse response, XContentBuilder builder) throws Exception {
                return new BytesRestResponse(RestStatus.OK, builder.startObject()
                        .field(Watch.Field.STATUS.getPreferredName(), response.getStatus(), WatcherParams.HIDE_SECRETS)
                        .endObject());
            }
        });
    }

    static class DeactivateRestHandler extends WatcherRestHandler {

        public DeactivateRestHandler(Settings settings, RestController controller, Client client) {
            super(settings, controller, client);
        }

        @Override
        protected void handleRequest(RestRequest request, RestChannel channel, WatcherClient client) throws Exception {
            String watchId = request.param("id");
            client.activateWatch(new ActivateWatchRequest(watchId, false), new RestBuilderListener<ActivateWatchResponse>(channel) {
                @Override
                public RestResponse buildResponse(ActivateWatchResponse response, XContentBuilder builder) throws Exception {
                    return new BytesRestResponse(RestStatus.OK, builder.startObject()
                            .field(Watch.Field.STATUS.getPreferredName(), response.getStatus(), WatcherParams.HIDE_SECRETS)
                            .endObject());
                }
            });
        }
    }

}
