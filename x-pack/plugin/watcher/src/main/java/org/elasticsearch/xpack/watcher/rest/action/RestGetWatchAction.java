/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.rest.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.core.watcher.client.WatcherClient;
import org.elasticsearch.xpack.core.watcher.transport.actions.get.GetWatchRequest;
import org.elasticsearch.xpack.core.watcher.transport.actions.get.GetWatchResponse;
import org.elasticsearch.xpack.watcher.rest.WatcherRestHandler;


import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import static org.elasticsearch.rest.RestStatus.OK;

public class RestGetWatchAction extends WatcherRestHandler {
    public RestGetWatchAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(GET, URI_BASE + "/watch/{id}", this);
    }

    @Override
    public String getName() {
        return "xpack_watcher_get_watch_action";
    }

    @Override
    protected RestChannelConsumer doPrepareRequest(final RestRequest request, WatcherClient client) {
        final GetWatchRequest getWatchRequest = new GetWatchRequest(request.param("id"));
        return channel -> client.getWatch(getWatchRequest, new RestBuilderListener<GetWatchResponse>(channel) {
            @Override
            public RestResponse buildResponse(GetWatchResponse response, XContentBuilder builder) throws Exception {
                builder.startObject()
                        .field("found", response.isFound())
                        .field("_id", response.getId());
                        if (response.isFound()) {
                            builder.field("_version", response.getVersion());
                            ToXContent.MapParams xContentParams = new ToXContent.MapParams(request.params());
                            builder.field("status", response.getStatus(), xContentParams);
                            builder.field("watch", response.getSource(), xContentParams);
                        }
                        builder.endObject();

                RestStatus status = response.isFound() ? OK : NOT_FOUND;
                return new BytesRestResponse(status, builder);
            }
        });
    }
}
