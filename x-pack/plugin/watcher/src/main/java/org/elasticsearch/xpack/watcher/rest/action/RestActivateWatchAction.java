/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.watcher.rest.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.watcher.support.xcontent.WatcherParams;
import org.elasticsearch.xpack.core.watcher.transport.actions.activate.ActivateWatchAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.activate.ActivateWatchRequest;
import org.elasticsearch.xpack.core.watcher.transport.actions.activate.ActivateWatchResponse;
import org.elasticsearch.xpack.core.watcher.watch.WatchField;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

/**
 * The rest action to de/activate a watch
 */
public class RestActivateWatchAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_watcher/watch/{id}/_activate"), new Route(PUT, "/_watcher/watch/{id}/_activate"));
    }

    @Override
    public String getName() {
        return "watcher_activate_watch";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        String watchId = request.param("id");
        return channel -> client.execute(
            ActivateWatchAction.INSTANCE,
            new ActivateWatchRequest(watchId, true),
            new RestBuilderListener<ActivateWatchResponse>(channel) {
                @Override
                public RestResponse buildResponse(ActivateWatchResponse response, XContentBuilder builder) throws Exception {
                    return new RestResponse(
                        RestStatus.OK,
                        builder.startObject()
                            .field(WatchField.STATUS.getPreferredName(), response.getStatus(), WatcherParams.HIDE_SECRETS)
                            .endObject()
                    );
                }
            }
        );
    }

    public static class DeactivateRestHandler extends BaseRestHandler {

        @Override
        public List<Route> routes() {
            return List.of(new Route(POST, "/_watcher/watch/{id}/_deactivate"), new Route(PUT, "/_watcher/watch/{id}/_deactivate"));
        }

        @Override
        public String getName() {
            return "watcher_deactivate_watch";
        }

        @Override
        public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
            String watchId = request.param("id");
            return channel -> client.execute(
                ActivateWatchAction.INSTANCE,
                new ActivateWatchRequest(watchId, false),
                new RestBuilderListener<ActivateWatchResponse>(channel) {
                    @Override
                    public RestResponse buildResponse(ActivateWatchResponse response, XContentBuilder builder) throws Exception {
                        return new RestResponse(
                            RestStatus.OK,
                            builder.startObject()
                                .field(WatchField.STATUS.getPreferredName(), response.getStatus(), WatcherParams.HIDE_SECRETS)
                                .endObject()
                        );
                    }
                }
            );
        }
    }

}
