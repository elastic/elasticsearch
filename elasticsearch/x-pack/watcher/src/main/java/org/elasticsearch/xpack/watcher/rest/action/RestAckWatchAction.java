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
import org.elasticsearch.xpack.watcher.transport.actions.ack.AckWatchRequest;
import org.elasticsearch.xpack.watcher.transport.actions.ack.AckWatchResponse;
import org.elasticsearch.xpack.watcher.watch.Watch;

/**
 * The rest action to ack a watch
 */
public class RestAckWatchAction extends WatcherRestHandler {

    @Inject
    public RestAckWatchAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.PUT, URI_BASE + "/watch/{id}/_ack", this);
        controller.registerHandler(RestRequest.Method.POST, URI_BASE + "/watch/{id}/_ack", this);
        controller.registerHandler(RestRequest.Method.POST, URI_BASE + "/watch/{id}/_ack/{actions}", this);
        controller.registerHandler(RestRequest.Method.PUT, URI_BASE + "/watch/{id}/_ack/{actions}", this);
        // these are going to be removed in 6.0
        controller.registerHandler(RestRequest.Method.PUT, URI_BASE + "/watch/{id}/{actions}/_ack", this);
        controller.registerHandler(RestRequest.Method.POST, URI_BASE + "/watch/{id}/{actions}/_ack", this);
    }

    @Override
    public void handleRequest(RestRequest request, RestChannel restChannel, WatcherClient client) throws Exception {
        AckWatchRequest ackWatchRequest = new AckWatchRequest(request.param("id"));
        String[] actions = request.paramAsStringArray("actions", null);
        if (actions != null) {
            ackWatchRequest.setActionIds(actions);
        }
        ackWatchRequest.masterNodeTimeout(request.paramAsTime("master_timeout", ackWatchRequest.masterNodeTimeout()));
        client.ackWatch(ackWatchRequest, new RestBuilderListener<AckWatchResponse>(restChannel) {
            @Override
            public RestResponse buildResponse(AckWatchResponse response, XContentBuilder builder) throws Exception {
                return new BytesRestResponse(RestStatus.OK, builder.startObject()
                        .field(Watch.Field.STATUS.getPreferredName(), response.getStatus(), WatcherParams.HIDE_SECRETS)
                        .endObject());

            }
        });
    }
    
}
