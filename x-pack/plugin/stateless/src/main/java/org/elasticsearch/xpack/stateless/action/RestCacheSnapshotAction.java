/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 * Handles {@code POST /_stateless/cache/snapshot?node_id={node_id}}. The {@code node_id}
 * query parameter is required and must identify a known node in the cluster; returns 400
 * if missing and 404 if the node cannot be found.
 */
public class RestCacheSnapshotAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "stateless_cache_snapshot";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_stateless/cache/snapshot"));
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String nodeId = request.param("node_id");
        if (nodeId == null || nodeId.isBlank()) {
            throw new IllegalArgumentException("required query parameter [node_id] is missing");
        }

        DiscoveryNode targetNode = client.admin().cluster().prepareState(TimeValue.THIRTY_SECONDS).get().getState().nodes().get(nodeId);
        if (targetNode == null) {
            throw new IllegalArgumentException("no node with id [" + nodeId + "] found in the cluster");
        }

        CacheSnapshotRequest cacheSnapshotRequest = new CacheSnapshotRequest(targetNode);
        return channel -> client.execute(
            TransportCacheSnapshotAction.TYPE,
            cacheSnapshotRequest,
            new RestBuilderListener<CacheSnapshotResponse>(channel) {
                @Override
                public RestResponse buildResponse(CacheSnapshotResponse response, XContentBuilder builder) throws Exception {
                    if (response.hasFailures()) {
                        builder.startObject();
                        builder.field("error", response.failures().get(0).getMessage());
                        builder.endObject();
                        return new RestResponse(RestStatus.INTERNAL_SERVER_ERROR, builder);
                    }
                    builder.startObject();
                    builder.field("snapshot_id", response.snapshotId());
                    builder.endObject();
                    return new RestResponse(OK, builder);
                }
            }
        );
    }
}
