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
import org.elasticsearch.xpack.core.watcher.transport.actions.ack.AckWatchRequest;
import org.elasticsearch.xpack.core.watcher.transport.actions.ack.AckWatchResponse;
import org.elasticsearch.xpack.core.watcher.watch.WatchField;
import org.elasticsearch.xpack.watcher.rest.WatcherRestHandler;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

/**
 * The rest action to ack a watch
 */
public class RestAckWatchAction extends WatcherRestHandler {
    private static final Logger logger = LogManager.getLogger(RestAckWatchAction.class);
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(logger);

    public RestAckWatchAction(Settings settings, RestController controller) {
        super(settings);

        controller.registerHandler(POST, URI_BASE + "/watch/{id}/_ack", this);
        controller.registerHandler(POST, "/_watcher/watch/{id}/_ack", this);
        controller.registerHandler(PUT, URI_BASE + "/watch/{id}/_ack", this);
        controller.registerHandler(PUT, "/_watcher/watch/{id}/_ack", this);
        controller.registerHandler(POST, URI_BASE + "/watch/{id}/_ack/{actions}", this);
        controller.registerHandler(POST, "/_watcher/watch/{id}/_ack/{actions}", this);
        controller.registerHandler(PUT, URI_BASE + "/watch/{id}/_ack/{actions}", this);
        controller.registerHandler(PUT, "/_watcher/watch/{id}/_ack/{actions}", this);

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
    public String getName() {
        return "xpack_watcher_ack_watch_action";
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
                        .field(WatchField.STATUS.getPreferredName(), response.getStatus(), WatcherParams.HIDE_SECRETS)
                        .endObject());

            }
        });
    }
    
}
