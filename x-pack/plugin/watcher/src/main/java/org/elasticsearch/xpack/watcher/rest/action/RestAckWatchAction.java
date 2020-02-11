/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.watcher.rest.action;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.core.watcher.client.WatcherClient;
import org.elasticsearch.xpack.core.watcher.support.xcontent.WatcherParams;
import org.elasticsearch.xpack.core.watcher.transport.actions.ack.AckWatchRequest;
import org.elasticsearch.xpack.core.watcher.transport.actions.ack.AckWatchResponse;
import org.elasticsearch.xpack.core.watcher.watch.WatchField;
import org.elasticsearch.xpack.watcher.rest.WatcherRestHandler;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

/**
 * The rest action to ack a watch
 */
public class RestAckWatchAction extends WatcherRestHandler {

    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(LogManager.getLogger(RestAckWatchAction.class));

    @Override
    public List<Route> routes() {
        return emptyList();
    }

    @Override
    public List<ReplacedRoute> replacedRoutes() {
        return unmodifiableList(asList(
            new ReplacedRoute(POST, "/_watcher/watch/{id}/_ack", POST, URI_BASE + "/watcher/watch/{id}/_ack", deprecationLogger),
            new ReplacedRoute(PUT, "/_watcher/watch/{id}/_ack", PUT, URI_BASE + "/watcher/watch/{id}/_ack", deprecationLogger),
            new ReplacedRoute(
                POST, "/_watcher/watch/{id}/_ack/{actions}",
                POST, URI_BASE + "/watcher/watch/{id}/_ack/{actions}", deprecationLogger),
            new ReplacedRoute(
                PUT, "/_watcher/watch/{id}/_ack/{actions}",
                PUT, URI_BASE + "/watcher/watch/{id}/_ack/{actions}", deprecationLogger)));
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
                return new BytesRestResponse(RestStatus.OK, builder.startObject()
                        .field(WatchField.STATUS.getPreferredName(), response.getStatus(), WatcherParams.HIDE_SECRETS)
                        .endObject());

            }
        });
    }
}
