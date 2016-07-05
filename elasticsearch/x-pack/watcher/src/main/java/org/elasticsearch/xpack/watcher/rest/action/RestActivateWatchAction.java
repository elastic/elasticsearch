/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.rest.action;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.support.RestBuilderListener;
import org.elasticsearch.xpack.watcher.client.WatcherClient;
import org.elasticsearch.xpack.watcher.rest.WatcherRestHandler;
import org.elasticsearch.xpack.watcher.support.xcontent.WatcherParams;
import org.elasticsearch.xpack.watcher.transport.actions.activate.ActivateWatchRequest;
import org.elasticsearch.xpack.watcher.transport.actions.activate.ActivateWatchResponse;
import org.elasticsearch.xpack.watcher.watch.Watch;

/**
 * The rest action to de/activate a watch
 */
public class RestActivateWatchAction extends WatcherRestHandler {

    @Inject
    public RestActivateWatchAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.PUT, URI_BASE + "/watch/{id}/_activate", this);
        controller.registerHandler(RestRequest.Method.POST, URI_BASE + "/watch/{id}/_activate", this);
        DeactivateRestHandler deactivateRestHandler = new DeactivateRestHandler(settings);
        controller.registerHandler(RestRequest.Method.PUT, URI_BASE + "/watch/{id}/_deactivate", deactivateRestHandler);
        controller.registerHandler(RestRequest.Method.POST, URI_BASE + "/watch/{id}/_deactivate", deactivateRestHandler);
    }

    @Override
    public void handleRequest(RestRequest request, RestChannel channel, WatcherClient client) throws Exception {
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

        public DeactivateRestHandler(Settings settings) {
            super(settings);
        }

        @Override
        public void handleRequest(RestRequest request, RestChannel channel, WatcherClient client) throws Exception {
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
