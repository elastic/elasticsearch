/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.rest.action;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestBuilderListener;
import org.elasticsearch.watcher.client.WatcherClient;
import org.elasticsearch.watcher.rest.WatcherRestHandler;
import org.elasticsearch.watcher.transport.actions.get.GetWatchRequest;
import org.elasticsearch.watcher.transport.actions.get.GetWatchResponse;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import static org.elasticsearch.rest.RestStatus.OK;

public class RestGetWatchAction extends WatcherRestHandler {

    @Inject
    public RestGetWatchAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(GET, URI_BASE + "/watch/{id}", this);
    }

    @Override
    protected void handleRequest(RestRequest request, RestChannel channel, WatcherClient client) throws Exception {
        final GetWatchRequest getWatchRequest = new GetWatchRequest(request.param("id"));
        client.getWatch(getWatchRequest, new RestBuilderListener<GetWatchResponse>(channel) {
            @Override
            public RestResponse buildResponse(GetWatchResponse response, XContentBuilder builder) throws Exception {
                builder.startObject()
                        .field("found", response.isFound())
                        .field("_id", response.getId())
                        .field("_version", response.getVersion())
                        .field("watch", response.getSourceAsMap())
                        .endObject();

                RestStatus status = response.isFound() ? OK : NOT_FOUND;
                return new BytesRestResponse(status, builder);
            }
        });
    }
}