/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.snapshots.globalstate.SnapshotGlobalStateAction;
import org.elasticsearch.action.admin.cluster.snapshots.globalstate.SnapshotGlobalStateRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestChunkedToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * Returns global state of a snapshot
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
        String repository = request.param("repository");
        String snapshot = request.param("snapshot");
        SnapshotGlobalStateRequest snapshotGlobalStateRequest = new SnapshotGlobalStateRequest(repository, snapshot);
        snapshotGlobalStateRequest.masterNodeTimeout(request.paramAsTime("master_timeout", snapshotGlobalStateRequest.masterNodeTimeout()));
        return restChannel -> client.execute(
            SnapshotGlobalStateAction.INSTANCE,
            snapshotGlobalStateRequest,
            new RestChunkedToXContentListener<>(restChannel)
        );
    }
}
