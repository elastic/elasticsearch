/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.rest;

import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotAction;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestMountSearchableSnapshotAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "mount_snapshot_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_snapshot/{repository}/{snapshot}/_mount"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        MountSearchableSnapshotRequest mountSearchableSnapshotRequest = MountSearchableSnapshotRequest.PARSER.apply(
            request.contentParser(),
            request
        ).masterNodeTimeout(request.paramAsTime("master_timeout", MasterNodeRequest.DEFAULT_MASTER_NODE_TIMEOUT));
        return channel -> client.execute(
            MountSearchableSnapshotAction.INSTANCE,
            mountSearchableSnapshotRequest,
            new RestToXContentListener<>(channel)
        );
    }
}
