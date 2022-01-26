/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.watcher.rest.action;

import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.watcher.client.WatcherClient;
import org.elasticsearch.xpack.core.watcher.support.xcontent.WatcherParams;
import org.elasticsearch.xpack.core.watcher.transport.actions.activate.ActivateWatchRequest;
import org.elasticsearch.xpack.core.watcher.transport.actions.activate.ActivateWatchResponse;
import org.elasticsearch.xpack.core.watcher.watch.WatchField;
import org.elasticsearch.xpack.watcher.rest.WatcherRestHandler;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

/**
 * The rest action to de/activate a watch
 */
public class RestActivateWatchAction extends WatcherRestHandler {

    @Override
    public List<Route> routes() {
        return unmodifiableList(
            asList(
                Route.builder(POST, "/_watcher/watch/{id}/_activate")
                    .replaces(POST, URI_BASE + "/watcher/watch/{id}/_activate", RestApiVersion.V_7)
                    .build(),
                Route.builder(PUT, "/_watcher/watch/{id}/_activate")
                    .replaces(PUT, URI_BASE + "/watcher/watch/{id}/_activate", RestApiVersion.V_7)
                    .build()
            )
        );
    }

    @Override
    public String getName() {
        return "watcher_activate_watch";
    }

    @Override
    public RestChannelConsumer doPrepareRequest(RestRequest request, WatcherClient client) {
        String watchId = request.param("id");
        return channel -> client.activateWatch(
            new ActivateWatchRequest(watchId, true),
            new RestBuilderListener<ActivateWatchResponse>(channel) {
                @Override
                public RestResponse buildResponse(ActivateWatchResponse response, XContentBuilder builder) throws Exception {
                    return new BytesRestResponse(
                        RestStatus.OK,
                        builder.startObject()
                            .field(WatchField.STATUS.getPreferredName(), response.getStatus(), WatcherParams.HIDE_SECRETS)
                            .endObject()
                    );
                }
            }
        );
    }

    public static class DeactivateRestHandler extends WatcherRestHandler {

        @Override
        public List<Route> routes() {
            return unmodifiableList(
                asList(
                    Route.builder(POST, "/_watcher/watch/{id}/_deactivate")
                        .replaces(POST, URI_BASE + "/watcher/watch/{id}/_deactivate", RestApiVersion.V_7)
                        .build(),
                    Route.builder(PUT, "/_watcher/watch/{id}/_deactivate")
                        .replaces(PUT, URI_BASE + "/watcher/watch/{id}/_deactivate", RestApiVersion.V_7)
                        .build()
                )
            );
        }

        @Override
        public String getName() {
            return "watcher_deactivate_watch";
        }

        @Override
        public RestChannelConsumer doPrepareRequest(RestRequest request, WatcherClient client) {
            String watchId = request.param("id");
            return channel -> client.activateWatch(
                new ActivateWatchRequest(watchId, false),
                new RestBuilderListener<ActivateWatchResponse>(channel) {
                    @Override
                    public RestResponse buildResponse(ActivateWatchResponse response, XContentBuilder builder) throws Exception {
                        return new BytesRestResponse(
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
