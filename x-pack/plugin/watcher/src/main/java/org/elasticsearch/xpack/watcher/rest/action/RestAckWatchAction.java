/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.watcher.rest.action;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.core.watcher.support.xcontent.WatcherParams;
import org.elasticsearch.xpack.core.watcher.transport.actions.ack.AckWatchAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.ack.AckWatchRequest;
import org.elasticsearch.xpack.core.watcher.transport.actions.ack.AckWatchResponse;
import org.elasticsearch.xpack.core.watcher.watch.WatchField;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

/**
 * The rest action to ack a watch
 */
public class RestAckWatchAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            Route.builder(POST, "/_watcher/watch/{id}/_ack")
                .replaces(POST, "/_xpack/watcher/watch/{id}/_ack", RestApiVersion.V_7).build(),
            Route.builder(PUT, "/_watcher/watch/{id}/_ack")
                .replaces(PUT, "/_xpack/watcher/watch/{id}/_ack", RestApiVersion.V_7).build(),
            Route.builder(POST, "/_watcher/watch/{id}/_ack/{actions}")
                .replaces(POST, "/_xpack/watcher/watch/{id}/_ack/{actions}", RestApiVersion.V_7).build(),
            Route.builder(PUT, "/_watcher/watch/{id}/_ack/{actions}")
                .replaces(PUT, "/_xpack/watcher/watch/{id}/_ack/{actions}", RestApiVersion.V_7).build()
        );
    }

    @Override
    public String getName() {
        return "watcher_ack_watch";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        AckWatchRequest ackWatchRequest = new AckWatchRequest(request.param("id"));
        String[] actions = request.paramAsStringArray("actions", null);
        if (actions != null) {
            ackWatchRequest.setActionIds(actions);
        }
        return channel -> client.execute(AckWatchAction.INSTANCE, ackWatchRequest, new RestBuilderListener<AckWatchResponse>(channel) {
            @Override
            public RestResponse buildResponse(AckWatchResponse response, XContentBuilder builder) throws Exception {
                return new BytesRestResponse(RestStatus.OK, builder.startObject()
                        .field(WatchField.STATUS.getPreferredName(), response.getStatus(), WatcherParams.HIDE_SECRETS)
                        .endObject());

            }
        });
    }
}
