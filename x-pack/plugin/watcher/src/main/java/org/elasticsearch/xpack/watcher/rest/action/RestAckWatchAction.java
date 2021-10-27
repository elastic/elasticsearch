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
import org.elasticsearch.xpack.core.watcher.transport.actions.ack.AckWatchRequest;
import org.elasticsearch.xpack.core.watcher.transport.actions.ack.AckWatchResponse;
import org.elasticsearch.xpack.core.watcher.watch.WatchField;
import org.elasticsearch.xpack.watcher.rest.WatcherRestHandler;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

/**
 * The rest action to ack a watch
 */
public class RestAckWatchAction extends WatcherRestHandler {

    @Override
    public List<Route> routes() {
        return unmodifiableList(
            asList(
                Route.builder(POST, "/_watcher/watch/{id}/_ack")
                    .replaces(POST, URI_BASE + "/watcher/watch/{id}/_ack", RestApiVersion.V_7)
                    .build(),
                Route.builder(PUT, "/_watcher/watch/{id}/_ack")
                    .replaces(PUT, URI_BASE + "/watcher/watch/{id}/_ack", RestApiVersion.V_7)
                    .build(),
                Route.builder(POST, "/_watcher/watch/{id}/_ack/{actions}")
                    .replaces(POST, URI_BASE + "/watcher/watch/{id}/_ack/{actions}", RestApiVersion.V_7)
                    .build(),
                Route.builder(PUT, "/_watcher/watch/{id}/_ack/{actions}")
                    .replaces(PUT, URI_BASE + "/watcher/watch/{id}/_ack/{actions}", RestApiVersion.V_7)
                    .build()
            )
        );
    }

    @Override
    public String getName() {
        return "watcher_ack_watch";
    }

    @Override
    public RestChannelConsumer doPrepareRequest(RestRequest request, WatcherClient client) {
        AckWatchRequest ackWatchRequest = new AckWatchRequest(request.param("id"));
        String[] actions = request.paramAsStringArray("actions", null);
        if (actions != null) {
            ackWatchRequest.setActionIds(actions);
        }
        return channel -> client.ackWatch(ackWatchRequest, new RestBuilderListener<AckWatchResponse>(channel) {
            @Override
            public RestResponse buildResponse(AckWatchResponse response, XContentBuilder builder) throws Exception {
                return new BytesRestResponse(
                    RestStatus.OK,
                    builder.startObject()
                        .field(WatchField.STATUS.getPreferredName(), response.getStatus(), WatcherParams.HIDE_SECRETS)
                        .endObject()
                );

            }
        });
    }
}
