/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.client.Requests.restoreSnapshotRequest;
import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * Restores a snapshot
 */
public class RestRestoreSnapshotAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_snapshot/{repository}/{snapshot}/_restore"));
    }

    @Override
    public String getName() {
        return "restore_snapshot_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        RestoreSnapshotRequest restoreSnapshotRequest = restoreSnapshotRequest(request.param("repository"), request.param("snapshot"));
        restoreSnapshotRequest.masterNodeTimeout(request.paramAsTime("master_timeout", restoreSnapshotRequest.masterNodeTimeout()));
        restoreSnapshotRequest.waitForCompletion(request.paramAsBoolean("wait_for_completion", false));
        request.applyContentParser(p -> restoreSnapshotRequest.source(p.mapOrdered()));
        return channel -> client.admin().cluster().restoreSnapshot(restoreSnapshotRequest, new RestToXContentListener<>(channel));
    }
}
