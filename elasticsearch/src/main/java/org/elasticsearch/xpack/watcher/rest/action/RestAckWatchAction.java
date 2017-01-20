/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.rest.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.watcher.client.WatcherClient;
import org.elasticsearch.xpack.watcher.rest.WatcherRestHandler;
import org.elasticsearch.xpack.watcher.support.xcontent.WatcherParams;
import org.elasticsearch.xpack.watcher.transport.actions.ack.AckWatchRequest;
import org.elasticsearch.xpack.watcher.transport.actions.ack.AckWatchResponse;
import org.elasticsearch.xpack.watcher.watch.Watch;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

/**
 * The rest action to ack a watch
 */
public class RestAckWatchAction extends WatcherRestHandler {
    public RestAckWatchAction(Settings settings, RestController controller) {
        super(settings);
        // @deprecated Remove deprecations in 6.0
        controller.registerWithDeprecatedHandler(POST, URI_BASE + "/watch/{id}/_ack", this,
                                                 POST, "/_watcher/watch/{id}/_ack", deprecationLogger);
        controller.registerWithDeprecatedHandler(PUT, URI_BASE + "/watch/{id}/_ack", this,
                                                 PUT, "/_watcher/watch/{id}/_ack", deprecationLogger);
        controller.registerWithDeprecatedHandler(POST, URI_BASE + "/watch/{id}/_ack/{actions}", this,
                                                 POST, "/_watcher/watch/{id}/_ack/{actions}", deprecationLogger);
        controller.registerWithDeprecatedHandler(PUT, URI_BASE + "/watch/{id}/_ack/{actions}", this,
                                                 PUT, "/_watcher/watch/{id}/_ack/{actions}", deprecationLogger);

        // @deprecated The following can be totally dropped in 6.0
        // Note: we deprecated "/{actions}/_ack" totally; so we don't replace it with a matching _xpack variant
        controller.registerAsDeprecatedHandler(POST, "/_watcher/watch/{id}/{actions}/_ack", this,
                                               "[POST /_watcher/watch/{id}/{actions}/_ack] is deprecated! Use " +
                                               "[POST /_xpack/watcher/watch/{id}/_ack/{actions}] instead.",
                                               deprecationLogger);
        controller.registerAsDeprecatedHandler(PUT, "/_watcher/watch/{id}/{actions}/_ack", this,
                                               "[PUT /_watcher/watch/{id}/{actions}/_ack] is deprecated! Use " +
                                               "[PUT /_xpack/watcher/watch/{id}/_ack/{actions}] instead.",
                                               deprecationLogger);
    }

    @Override
    public RestChannelConsumer doPrepareRequest(RestRequest request, WatcherClient client) throws IOException {
        AckWatchRequest ackWatchRequest = new AckWatchRequest(request.param("id"));
        String[] actions = request.paramAsStringArray("actions", null);
        if (actions != null) {
            ackWatchRequest.setActionIds(actions);
        }
        ackWatchRequest.masterNodeTimeout(request.paramAsTime("master_timeout", ackWatchRequest.masterNodeTimeout()));
        return channel -> client.ackWatch(ackWatchRequest, new RestBuilderListener<AckWatchResponse>(channel) {
            @Override
            public RestResponse buildResponse(AckWatchResponse response, XContentBuilder builder) throws Exception {
                return new BytesRestResponse(RestStatus.OK, builder.startObject()
                        .field(Watch.Field.STATUS.getPreferredName(), response.getStatus(), WatcherParams.HIDE_SECRETS)
                        .endObject());

            }
        });
    }
    
}
