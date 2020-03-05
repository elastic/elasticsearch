/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.searchablesnapshots.rest;

import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.searchablesnapshots.action.MountSearchableSnapshotAction;
import org.elasticsearch.xpack.searchablesnapshots.action.MountSearchableSnapshotRequest;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestMountSearchableSnapshotAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "mount_snapshot_action";
    }

    @Override
    public List<Route> routes() {
        return Collections.singletonList(new Route(POST, "/{index}/_searchable_snapshots/mount"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        MountSearchableSnapshotRequest mountSearchableSnapshotRequest = MountSearchableSnapshotRequest.PARSER.apply(request.contentParser(),
            new MountSearchableSnapshotRequest.RequestParams(
                request.param("index"),
                request.paramAsTime("master_timeout", MasterNodeRequest.DEFAULT_MASTER_NODE_TIMEOUT),
                request.paramAsBoolean("wait_for_completion", false)));
        return channel -> client.execute(MountSearchableSnapshotAction.INSTANCE, mountSearchableSnapshotRequest,
            new RestToXContentListener<>(channel));
    }
}
