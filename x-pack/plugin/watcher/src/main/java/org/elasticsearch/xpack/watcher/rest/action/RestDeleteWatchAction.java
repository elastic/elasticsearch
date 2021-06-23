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
import org.elasticsearch.protocol.xpack.watcher.DeleteWatchRequest;
import org.elasticsearch.protocol.xpack.watcher.DeleteWatchResponse;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.core.watcher.transport.actions.delete.DeleteWatchAction;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;
import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import static org.elasticsearch.rest.RestStatus.OK;

public class RestDeleteWatchAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            Route.builder(DELETE, "/_watcher/watch/{id}")
                .replaces(DELETE, "/_xpack/watcher/watch/{id}", RestApiVersion.V_7).build()
        );
    }

    @Override
    public String getName() {
        return "watcher_delete_watch";
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, NodeClient client) {
        DeleteWatchRequest deleteWatchRequest = new DeleteWatchRequest(request.param("id"));
        return channel -> client.execute(DeleteWatchAction.INSTANCE, deleteWatchRequest, new RestBuilderListener<>(channel) {
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
