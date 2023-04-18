/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.snapshots.clone.CloneSnapshotRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.PUT;

/**
 * Clones indices from one snapshot into another snapshot in the same repository
 */
public class RestCloneSnapshotAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, "/_snapshot/{repository}/{snapshot}/_clone/{target_snapshot}"));
    }

    @Override
    public String getName() {
        return "clone_snapshot_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final Map<String, Object> source = request.contentParser().map();
        final CloneSnapshotRequest cloneSnapshotRequest = new CloneSnapshotRequest(
            request.param("repository"),
            request.param("snapshot"),
            request.param("target_snapshot"),
            XContentMapValues.nodeStringArrayValue(source.getOrDefault("indices", Collections.emptyList()))
        );
        cloneSnapshotRequest.masterNodeTimeout(request.paramAsTime("master_timeout", cloneSnapshotRequest.masterNodeTimeout()));
        cloneSnapshotRequest.indicesOptions(IndicesOptions.fromMap(source, cloneSnapshotRequest.indicesOptions()));
        return channel -> client.admin().cluster().cloneSnapshot(cloneSnapshotRequest, new RestToXContentListener<>(channel));
    }
}
