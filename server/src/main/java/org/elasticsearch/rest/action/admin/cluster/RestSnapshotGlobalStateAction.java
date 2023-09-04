/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.snapshots.globalstate.SnapshotGlobalStateRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * Restores a snapshot
 */
public class RestSnapshotGlobalStateAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_snapshot/{repository}/{snapshot}/_global_state"));
    }

    @Override
    public String getName() {
        return "global_state_snapshot_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        System.out.println("Preparing Request");
        String repository = request.param("repository");
        String snapshot = request.param("snapshot");
        SnapshotGlobalStateRequest snapshotGlobalStateRequest = new SnapshotGlobalStateRequest(repository, snapshot);
        snapshotGlobalStateRequest.masterNodeTimeout(request.paramAsTime("master_timeout", snapshotGlobalStateRequest.masterNodeTimeout()));
        // return channel -> client.admin().cluster().snapshotGlobalState(snapshotGlobalStateRequest, new
        // RestToXContentListener<>(channel));
        return channel -> new RestCancellableNodeClient(client, request.getHttpChannel()).admin()
            .cluster()
            .snapshotGlobalState(snapshotGlobalStateRequest, new RestToXContentListener<>(channel));

    }
}
