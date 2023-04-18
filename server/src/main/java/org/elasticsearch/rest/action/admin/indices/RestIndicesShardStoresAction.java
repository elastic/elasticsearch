/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresAction;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestChunkedToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * Rest action for {@link IndicesShardStoresAction}
 */
public class RestIndicesShardStoresAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_shard_stores"), new Route(GET, "/{index}/_shard_stores"));
    }

    @Override
    public String getName() {
        return "indices_shard_stores_action";
    }

    @Override
    public boolean allowSystemIndexAccessByDefault() {
        return true;
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        IndicesShardStoresRequest indicesShardStoresRequest = new IndicesShardStoresRequest(
            Strings.splitStringByCommaToArray(request.param("index"))
        );
        if (request.hasParam("status")) {
            indicesShardStoresRequest.shardStatuses(Strings.splitStringByCommaToArray(request.param("status")));
        }
        indicesShardStoresRequest.maxConcurrentShardRequests(
            request.paramAsInt("max_concurrent_shard_requests", indicesShardStoresRequest.maxConcurrentShardRequests())
        );
        indicesShardStoresRequest.indicesOptions(IndicesOptions.fromRequest(request, indicesShardStoresRequest.indicesOptions()));
        return channel -> new RestCancellableNodeClient(client, request.getHttpChannel()).admin()
            .indices()
            .shardStores(indicesShardStoresRequest, new RestChunkedToXContentListener<>(channel));
    }
}
