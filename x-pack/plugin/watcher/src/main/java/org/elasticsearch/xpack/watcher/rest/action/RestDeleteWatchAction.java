/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.watcher.rest.action;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.core.watcher.client.WatcherClient;
import org.elasticsearch.protocol.xpack.watcher.DeleteWatchRequest;
import org.elasticsearch.protocol.xpack.watcher.DeleteWatchResponse;
import org.elasticsearch.xpack.watcher.rest.WatcherRestHandler;

import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.rest.RestRequest.Method.DELETE;
import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import static org.elasticsearch.rest.RestStatus.OK;

public class RestDeleteWatchAction extends WatcherRestHandler {

    @Override
    public List<Route> routes() {
        return emptyList();
    }

    @Override
    public List<ReplacedRoute> replacedRoutes() {
        return singletonList(
            new ReplacedRoute(DELETE, "/_watcher/watch/{id}", DELETE, URI_BASE + "/watcher/watch/{id}"));
    }

    @Override
    public String getName() {
        return "watcher_delete_watch";
    }

    @Override
    protected RestChannelConsumer doPrepareRequest(final RestRequest request, WatcherClient client) {
        DeleteWatchRequest deleteWatchRequest = new DeleteWatchRequest(request.param("id"));
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
