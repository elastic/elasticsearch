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
import org.elasticsearch.watcher.transport.actions.delete.DeleteWatchRequest;
import org.elasticsearch.watcher.transport.actions.delete.DeleteWatchResponse;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;
import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 */
public class RestDeleteWatchAction extends WatcherRestHandler {

    @Inject
    public RestDeleteWatchAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(DELETE, URI_BASE + "/watch/{id}", this);
    }

    @Override
    protected void handleRequest(final RestRequest request, RestChannel channel, WatcherClient client) throws Exception {
        DeleteWatchRequest indexWatchRequest = new DeleteWatchRequest(request.param("id"));
        client.deleteWatch(indexWatchRequest, new RestBuilderListener<DeleteWatchResponse>(channel) {
            @Override
            public RestResponse buildResponse(DeleteWatchResponse response, XContentBuilder builder) throws Exception {
                builder.startObject()
                        .field("_id", response.getId())
                        .field("_version", response.getVersion())
                        .field("found", response.isFound())
                        .endObject();
                RestStatus status = response.isFound() ? OK : NOT_FOUND;
                return new BytesRestResponse(status, builder);
            }
        });
    }
}
