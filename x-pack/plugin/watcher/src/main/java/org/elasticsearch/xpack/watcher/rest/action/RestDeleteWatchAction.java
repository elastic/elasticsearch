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
import org.elasticsearch.protocol.xpack.watcher.DeleteWatchRequest;
import org.elasticsearch.protocol.xpack.watcher.DeleteWatchResponse;
import org.elasticsearch.xpack.watcher.rest.WatcherRestHandler;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;
import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import static org.elasticsearch.rest.RestStatus.OK;

public class RestDeleteWatchAction extends WatcherRestHandler {
    private static final Logger logger = LogManager.getLogger(RestDeleteWatchAction.class);
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(logger);

    public RestDeleteWatchAction(Settings settings, RestController controller) {
        super(settings);

        controller.registerHandler(DELETE, URI_BASE + "/watch/{id}", this);
        controller.registerHandler(DELETE, "/_watcher/watch/{id}", this);
    }

    @Override
    public String getName() {
        return "xpack_watcher_delete_watch_action";
    }

    @Override
    protected RestChannelConsumer doPrepareRequest(final RestRequest request, WatcherClient client) throws IOException {
        DeleteWatchRequest deleteWatchRequest = new DeleteWatchRequest(request.param("id"));
        deleteWatchRequest.masterNodeTimeout(request.paramAsTime("master_timeout", deleteWatchRequest.masterNodeTimeout()));
        return channel -> client.deleteWatch(deleteWatchRequest, new RestBuilderListener<DeleteWatchResponse>(channel) {
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
