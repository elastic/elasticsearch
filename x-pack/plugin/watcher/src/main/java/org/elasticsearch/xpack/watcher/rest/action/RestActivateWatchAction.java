/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.watcher.rest.action;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.core.watcher.support.xcontent.WatcherParams;
import org.elasticsearch.xpack.core.watcher.transport.actions.activate.ActivateWatchAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.activate.ActivateWatchRequest;
import org.elasticsearch.xpack.core.watcher.transport.actions.activate.ActivateWatchResponse;
import org.elasticsearch.xpack.core.watcher.watch.WatchField;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

/**
 * The rest action to de/activate a watch
 */
public class RestActivateWatchAction extends BaseRestHandler {

    public RestActivateWatchAction(RestController controller) {
        controller.registerHandler(POST, "/_watcher/watch/{id}/_activate", this);
        controller.registerHandler(PUT, "/_watcher/watch/{id}/_activate", this);

        final DeactivateRestHandler deactivateRestHandler = new DeactivateRestHandler();
        controller.registerHandler(POST, "/_watcher/watch/{id}/_deactivate", deactivateRestHandler);
        controller.registerHandler(PUT, "/_watcher/watch/{id}/_deactivate", deactivateRestHandler);
    }

    @Override
    public String getName() {
        return "watcher_activate_watch";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        String watchId = request.param("id");
        return channel ->
                client.execute(ActivateWatchAction.INSTANCE, new ActivateWatchRequest(watchId, true),
                    new RestBuilderListener<ActivateWatchResponse>(channel) {
                        @Override
                        public RestResponse buildResponse(ActivateWatchResponse response, XContentBuilder builder) throws Exception {
                            return new BytesRestResponse(RestStatus.OK, builder.startObject()
                                    .field(WatchField.STATUS.getPreferredName(), response.getStatus(), WatcherParams.HIDE_SECRETS)
                                    .endObject());
                        }
                    });
    }

    private static class DeactivateRestHandler extends BaseRestHandler {

        DeactivateRestHandler() {
        }

        @Override
        public String getName() {
            return "watcher_deactivate_watch";
        }

        @Override
        public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
            String watchId = request.param("id");
            return channel ->
                    client.execute(ActivateWatchAction.INSTANCE, new ActivateWatchRequest(watchId, false),
                        new RestBuilderListener<ActivateWatchResponse>(channel) {
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
