/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

/**
 * Deletes a snapshot
 */
public class RestDeleteSnapshotAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(DELETE, "/_snapshot/{repository}/{snapshot}"));
    }

    @Override
    public String getName() {
        return "delete_snapshot_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        String repository = request.param("repository");
        String[] snapshots = Strings.splitStringByCommaToArray(request.param("snapshot"));
        DeleteSnapshotRequest deleteSnapshotRequest = new DeleteSnapshotRequest(repository, snapshots);
        deleteSnapshotRequest.masterNodeTimeout(request.paramAsTime("master_timeout", deleteSnapshotRequest.masterNodeTimeout()));
        return channel -> client.admin().cluster().deleteSnapshot(deleteSnapshotRequest, new RestToXContentListener<>(channel));
    }
}
